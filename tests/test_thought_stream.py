import copy
import json
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from src.converter.antigravity_fix import normalize_antigravity_request
from src.converter.anti_truncation import AntiTruncationStreamProcessor, SYNTHETIC_TOOL_NAME
from src.converter.openai2gemini import convert_gemini_to_openai_stream
from src.converter.anthropic2gemini import gemini_stream_to_anthropic_stream
from src.router.antigravity import gemini
from src.stream_metadata import StreamMetadata


def chunk(parts, finish=None):
    candidate = {"content": {"role": "model", "parts": parts}}
    if finish:
        candidate["finishReason"] = finish
    return {"response": {"candidates": [candidate], "responseId": "synthetic-test-response"}}


def sse(data):
    return ("data: " + json.dumps(data) + "\n\n").encode()


async def source(items, fail=False):
    for item in items:
        yield sse(item)
    if fail:
        raise RuntimeError("synthetic interruption")


def parts_from(events):
    parts = []
    for event in events:
        raw = event.decode() if isinstance(event, bytes) else event
        if not raw.startswith("data: ") or raw[6:].strip() == "[DONE]":
            continue
        data = json.loads(raw[6:]); data = data.get("response", data)
        for candidate in data.get("candidates", []):
            parts.extend(candidate.get("content", {}).get("parts", []))
    return parts


class ThoughtStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_diagnostics_only_capture_metadata_and_never_mutate_stream(self):
        item = chunk([{"text": "private-synthetic-content", "thought": True}, {"text": "answer"}])
        raw = sse(item)
        metadata = StreamMetadata()
        metadata.observe(raw)
        for malformed in (b"data: [DONE]", "data: null", "data: []", 'data: {"response": null}', 'data: {"candidates":[null,{"content":null}]}'):
            metadata.observe(malformed)
        self.assertEqual(metadata.thought_parts, 1)
        self.assertEqual(metadata.text_parts, 1)
        self.assertNotIn("private-synthetic-content", json.dumps(metadata.as_dict()))
        self.assertEqual(raw, sse(item))

    async def test_global_and_request_thought_controls(self):
        for enabled, requested, expected in [(True, None, True), (True, False, False), (True, True, True), (False, True, False), (False, None, False)]:
            payload = {"model": "gemini-4-argon-medium", "contents": [{"role": "user", "parts": [{"text": "test"}]}]}
            if requested is not None:
                payload["generationConfig"] = {"thinkingConfig": {"includeThoughts": requested}}
            before = copy.deepcopy(payload)
            with patch("config.get_return_thoughts_to_frontend", AsyncMock(return_value=enabled)), \
                 patch("src.converter.antigravity_fix._get_model_rewrite_rules", AsyncMock(return_value=[])), \
                 patch("src.converter.antigravity_fix._get_sensitive_word_patterns", AsyncMock(return_value=[])):
                result = await normalize_antigravity_request(payload)
            self.assertIs(result["generationConfig"]["thinkingConfig"]["includeThoughts"], expected)
            self.assertEqual(payload, before)

    async def test_gemini_route_preserves_thought_and_does_not_guess_from_words(self):
        items = [chunk([{"text": "synthetic-thought", "thought": True}]),
                 chunk([{"text": "Thinking Process is the title of this book."}], "STOP")]
        async def mock_stream(**kwargs):
            async for event in source(items):
                yield event
        async def identity(payload):
            return payload
        app = FastAPI(); app.include_router(gemini.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: "test"
        with patch("src.api.antigravity.stream_request", mock_stream), \
             patch("src.converter.antigravity_fix.normalize_antigravity_request", identity):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                response = await client.post("/v1beta/models/gemini-4-argon-medium:streamGenerateContent", json={"contents": [{"role": "user", "parts": [{"text": "test"}]}]})
        self.assertEqual(response.status_code, 200)
        parts = parts_from(response.text.splitlines())
        self.assertEqual(parts, [p for item in items for p in item["response"]["candidates"][0]["content"]["parts"]])

    async def test_continuation_history_never_contains_thought_text(self):
        for interrupted in (False, True):
            payloads = []
            async def request(payload):
                payloads.append(copy.deepcopy(payload))
                if len(payloads) == 1:
                    return StreamingResponse(source([chunk([{"text": "synthetic-thought", "thought": True}]), chunk([{"text": "answer-prefix"}], None if interrupted else "MAX_TOKENS")], fail=interrupted))
                return StreamingResponse(source([chunk([{"text": "answer-suffix"}], "STOP")]))
            processor = AntiTruncationStreamProcessor(request, {"request": {"contents": [{"role": "user", "parts": [{"text": "test"}]}]}}, 2)
            events = [event async for event in processor.process_stream()]
            self.assertEqual(len(payloads), 2)
            self.assertNotIn("synthetic-thought", json.dumps(payloads[1]))
            self.assertIn("answer-prefix", json.dumps(payloads[1]))
            parts = parts_from(events)
            self.assertEqual([p["text"] for p in parts if p.get("thought")], ["synthetic-thought"])
            self.assertEqual("".join(p.get("text", "") for p in parts if not p.get("thought")), "answer-prefixanswer-suffix")

    async def test_thought_only_terminal_chunk_is_preserved(self):
        async def request(_):
            return StreamingResponse(source([chunk([{"text": "synthetic-thought", "thought": True}], "STOP")]))
        events = [event async for event in AntiTruncationStreamProcessor(request, {"request": {}}, 1).process_stream()]
        self.assertEqual(parts_from(events), [{"text": "synthetic-thought", "thought": True}])

    async def test_synthetic_answer_does_not_drop_later_thought(self):
        async def request(_):
            return StreamingResponse(source([
                chunk([{"functionCall": {"name": SYNTHETIC_TOOL_NAME, "args": {"content": "answer"}}}]),
                chunk([{"text": "synthetic-thought", "thought": True}, {"text": "duplicate-answer"}], "STOP")]))
        events = [event async for event in AntiTruncationStreamProcessor(request, {"request": {}}, 1).process_stream()]
        self.assertEqual(parts_from(events), [{"text": "answer"}, {"text": "synthetic-thought", "thought": True}])

    async def test_other_protocols_keep_thought_out_of_answer(self):
        item = chunk([{"text": "synthetic-thought", "thought": True}, {"text": "answer"}], "STOP")
        converted = convert_gemini_to_openai_stream(sse(item).decode(), "gemini-4-argon-medium", "test")
        delta = json.loads(converted[6:])["choices"][0]["delta"]
        self.assertEqual(delta["content"], "answer")
        self.assertEqual(delta["reasoning_content"], "synthetic-thought")
        events = [x async for x in gemini_stream_to_anthropic_stream(source([item]), "gemini-4-argon-medium")]
        data = []
        for event in events:
            raw = event.decode() if isinstance(event, bytes) else event
            data.extend(json.loads(line[6:]) for line in raw.splitlines() if line.startswith("data: "))
        deltas = [x["delta"] for x in data if x.get("type") == "content_block_delta"]
        self.assertIn({"type": "thinking_delta", "thinking": "synthetic-thought"}, deltas)
        self.assertIn({"type": "text_delta", "text": "answer"}, deltas)
