import copy
import json
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

from fastapi import Response
from src.thought_boundary import ThoughtBoundaryGuard, prepare_thought_boundary, ERROR_CODE
from src.api import antigravity as api
from src.converter.openai2gemini import convert_gemini_to_openai_stream
from src.converter.anthropic2gemini import gemini_stream_to_anthropic_stream

MARKER = "<<<AGY_FINAL_test>>>"


def event(parts, finish=None, wrapped=True):
    candidate = {"content": {"role": "model", "parts": parts}}
    if finish:
        candidate["finishReason"] = finish
    value = {"candidates": [candidate], "responseId": "boundary-test", "usageMetadata": {"totalTokenCount": 123}}
    return "data: " + json.dumps({"response": value} if wrapped else value) + "\n\n"


def decode(value):
    if isinstance(value, bytes):
        value = value.decode()
    obj = json.loads(value[6:])
    return obj.get("response", obj)


def text_of(chunks, thought=False):
    return "".join(p.get("text", "") for c in chunks if c is not None and not isinstance(c, Response)
                   for candidate in decode(c).get("candidates", [])
                   for p in candidate.get("content", {}).get("parts", [])
                   if bool(p.get("thought")) == thought)


class ThoughtBoundaryTests(unittest.IsolatedAsyncioTestCase):
    def test_every_delimiter_split_preserves_text_and_usage(self):
        for split in range(len(MARKER) + 1):
            guard = ThoughtBoundaryGuard(MARKER)
            chunks = [guard.feed(event([{"text": "Thinking Process:\n分析里也有中文。" + MARKER[:split]}])),
                      guard.feed(event([{"text": MARKER[split:] + "正文。English is allowed."}], "STOP"))]
            self.assertEqual(text_of(chunks, True), "Thinking Process:\n分析里也有中文。")
            self.assertEqual(text_of(chunks), "正文。English is allowed.")
            self.assertTrue(guard.complete)
            self.assertFalse(guard.failed)
            self.assertEqual(decode(chunks[-1])["usageMetadata"], {"totalTokenCount": 123})
            self.assertEqual(decode(chunks[-1])["candidates"][0]["finishReason"], "STOP")

    def test_one_character_chunks_do_not_leak_marker(self):
        guard = ThoughtBoundaryGuard(MARKER)
        result = [guard.feed(event([{"text": char}])) for char in "分析" + MARKER + "答案"]
        result.append(guard.feed(event([], "STOP")))
        self.assertEqual(text_of(result, True), "分析")
        self.assertEqual(text_of(result), "答案")

    def test_no_language_or_heading_guesses(self):
        guard = ThoughtBoundaryGuard(MARKER)
        result = [guard.feed(event([{"text": "Thinking Process:\n苏晨开始故事。\n\nFinal Answer: still unframed"}], "STOP"))]
        self.assertEqual(text_of(result), "")
        self.assertTrue(guard.failed)
        self.assertNotIn("finishReason", decode(result[0])["candidates"][0])
        self.assertEqual(json.loads(guard.error_response().body)["error"]["status"], ERROR_CODE)

    def test_native_thought_and_signature_preserved(self):
        guard = ThoughtBoundaryGuard(MARKER)
        part = {"text": "native analysis", "thought": True, "thoughtSignature": "synthetic"}
        result = guard.feed(event([part, {"text": MARKER + "answer"}], "STOP"))
        self.assertEqual(decode(result)["candidates"][0]["content"]["parts"][0], part)
        self.assertEqual(text_of([result]), "answer")

    def test_include_false_suppresses_both_native_and_unmarked_analysis(self):
        guard = ThoughtBoundaryGuard(MARKER, False)
        result = guard.feed(event([{"text": "native", "thought": True}, {"text": "unmarked" + MARKER + "answer"}], "STOP"))
        self.assertEqual(text_of([result], True), "")
        self.assertEqual(text_of([result]), "answer")

    def test_preparation_preserves_user_content_and_existing_system(self):
        req = {"contents": [{"role": "user", "parts": [{"text": "private story"}]}],
               "systemInstruction": {"parts": [{"text": "existing rules"}]}}
        before = copy.deepcopy(req)
        prepared, marker = prepare_thought_boundary("gemini-4-argon-medium", req)
        self.assertEqual(req, before)
        self.assertEqual(prepared["contents"], before["contents"])
        self.assertEqual(prepared["systemInstruction"]["parts"][0], {"text": "existing rules"})
        self.assertIn(marker, prepared["systemInstruction"]["parts"][1]["text"])
        self.assertEqual(prepare_thought_boundary("gemini-4-argon-medium", req)[1], marker)

    def test_other_models_tools_and_structured_outputs_unchanged(self):
        cases = [("claude-sonnet", {}), ("gemini-4-argon-medium", {"tools": [{"functionDeclarations": []}]}),
                 ("gemini-4-argon-medium", {"generationConfig": {"responseMimeType": "application/json"}}),
                 ("gemini-4-argon-medium", {"generationConfig": {"responseSchema": {"type": "OBJECT"}}})]
        for model, request in cases:
            result, marker = prepare_thought_boundary(model, request)
            self.assertIsNone(marker)
            self.assertEqual(result, request)

    async def test_protocol_converters_keep_reclassified_text_out_of_body(self):
        guard = ThoughtBoundaryGuard(MARKER)
        item = guard.feed(event([{"text": "unmarked analysis" + MARKER + "final answer"}], "STOP"))
        converted = convert_gemini_to_openai_stream(item, "gemini-4-argon-medium", "test")
        delta = json.loads(converted[6:])["choices"][0]["delta"]
        self.assertEqual(delta["content"], "final answer")
        self.assertEqual(delta["reasoning_content"], "unmarked analysis")
        async def source():
            yield item.encode()
        output = [x async for x in gemini_stream_to_anthropic_stream(source(), "gemini-4-argon-medium")]
        deltas = [json.loads(line[6:]).get("delta") for x in output for line in x.decode().splitlines() if line.startswith("data: ")]
        self.assertIn({"type": "thinking_delta", "thinking": "unmarked analysis"}, deltas)
        self.assertIn({"type": "text_delta", "text": "final answer"}, deltas)

    async def test_anthropic_boundary_error_is_a_real_error_event(self):
        async def source():
            yield ThoughtBoundaryGuard(MARKER).error_response()
        output = [x async for x in gemini_stream_to_anthropic_stream(source(), "gemini-4-argon-medium")]
        self.assertTrue(output[0].startswith(b"event: error\n"))
        self.assertIn(ERROR_CODE.encode(), output[0])

    async def test_api_integration_and_missing_boundary_eof(self):
        for framed in (True, False):
            async def source(**kwargs):
                prompt = kwargs["body"]["request"]["systemInstruction"]["parts"][-1]["text"]
                import re
                marker = re.search(r"<<<AGY_FINAL_[0-9a-f]+>>>", prompt)[0]
                yield event([{"text": "unmarked analysis"}])
                if framed:
                    yield event([{"text": marker + "answer"}], "STOP")
            with ExitStack() as stack:
                for name, value in {
                    "get_valid_credential": ("synthetic@example.invalid", {"access_token": "synthetic", "project_id": "test"}),
                    "get_antigravity_api_url": "https://example.invalid", "get_auto_ban_error_codes": [],
                    "get_retry_config": {"max_retries": 0, "retry_interval": 0, "retry_enabled": False},
                    "record_api_call_success": None, "_record_request_body_detail": "",
                }.items():
                    stack.enter_context(patch.object(api, name, AsyncMock(return_value=value)))
                saved = stack.enter_context(patch("src.usage_log.record_usage_log", AsyncMock()))
                stack.enter_context(patch.object(api, "stream_post_async", source))
                chunks = [x async for x in api.stream_request({"model": "gemini-4-argon-medium", "request": {"contents": []}}, native=False)]
            self.assertEqual(text_of(chunks, True), "unmarked analysis")
            if framed:
                self.assertEqual(text_of(chunks), "answer")
                self.assertTrue(saved.call_args.args[0]["success"])
            else:
                self.assertEqual(text_of(chunks), "")
                self.assertIsInstance(chunks[-1], Response)
                self.assertEqual(chunks[-1].status_code, 502)
                self.assertFalse(saved.call_args.args[0]["success"])
