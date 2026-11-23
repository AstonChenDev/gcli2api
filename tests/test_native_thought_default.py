"""Default requests use upstream thought fields, never private prompt framing."""
import copy
import json
import os
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI, Response

import test_custom_audit
from src.api import antigravity as api
from src.router.antigravity import anthropic, gemini, openai
from stream_fixtures import event, text_of


class NativeThoughtDefaultTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await test_custom_audit.CustomAuditTests.asyncSetUp(self)
        self.stack.enter_context(patch.dict(os.environ))
        os.environ.pop("ARGON_THOUGHT_BOUNDARY_ENABLED", None)

    async def test_retired_flags_cannot_inject_or_reclassify_output(self):
        source = ("Thinking Process:\n1. **Analyze Request:** check constraints.\n"
                  "2. **Writing and Polishing:** plan the answer.\n"
                  "<<<AGY_FINAL_literal>>>正文内的分析保持原样。<think>字面标签</think>")
        request = {"contents": [{"role": "user", "parts": [{"text": "test"}]}],
                   "system_instruction": {"parts": [{"text": "Keep my original rules."}]}}
        original = copy.deepcopy(request)
        baseline = None
        for setting in (None, "true", "false", " TRUE "):
            sent = []
            async def upstream(**kwargs):
                sent.append(copy.deepcopy(kwargs["body"]))
                yield event([{"text": "native reasoning", "thought": True}])
                for char in source:
                    yield event([{"text": char}])
                yield event([], "STOP")
            with patch.dict(os.environ):
                for key in ("ARGON_THOUGHT_BOUNDARY_ENABLED", "ARGON_PASSIVE_THOUGHT_RECOVERY_ENABLED"):
                    if setting is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = setting
                with patch.object(api, "stream_post_async", upstream), patch.object(
                        api, "_generate_request_id", return_value="fixed-test-id"):
                    chunks = [c async for c in api.stream_request({"model": "gemini-4-argon-medium", "request": request})]
            self.assertEqual(len(sent), 1)
            self.assertEqual(sent[0]["request"]["systemInstruction"], original["system_instruction"])
            self.assertNotIn("Output transport contract", json.dumps(sent))
            self.assertEqual(request, original)
            self.assertEqual(text_of(chunks), source)
            self.assertEqual(text_of(chunks, True), "native reasoning")
            if baseline is None:
                baseline = sent[0]
            self.assertEqual(sent[0], baseline)

    async def test_three_stream_protocols_with_thought_controls_and_continuation(self):
        app = FastAPI()
        for module in (gemini, openai, anthropic):
            app.include_router(module.router)
        app.dependency_overrides[gemini.authenticate_gemini_flexible] = lambda: "test"
        app.dependency_overrides[openai.authenticate_bearer] = lambda: "test"
        cases = [
            ("gemini", "/v1beta/models/gemini-4-argon-medium:streamGenerateContent",
             {"contents": [{"role": "user", "parts": [{"text": "test"}]}]}),
            ("openai", "/v1/chat/completions",
             {"model": "gemini-4-argon-medium", "stream": True,
              "messages": [{"role": "user", "content": "test"}]}),
            ("anthropic", "/v1/messages",
             {"model": "gemini-4-argon-medium", "stream": True, "max_tokens": 1024,
              "messages": [{"role": "user", "content": "test"}]}),
        ]
        answer = "Thinking Process is a book title. 正常正文。"
        for protocol, url, body in cases:
            for include in (True, False):
                for anti in (True, False):
                    with self.subTest(protocol=protocol, include=include, anti=anti):
                        calls = []
                        async def upstream(**kwargs):
                            payload = kwargs["body"]
                            calls.append(copy.deepcopy(payload))
                            self.assertNotIn("Output transport contract", json.dumps(payload))
                            self.assertNotIn("<<<AGY_FINAL_", json.dumps(payload))
                            self.assertIs(payload["request"]["generationConfig"]["thinkingConfig"]["includeThoughts"], include)
                            yield event([{"thought": True, "thoughtSignature": "hidden-signature"}])
                            # Like upstream, preserve returned thought fields even if it ignores the preference.
                            yield event([{"text": "native reasoning", "thought": True}])
                            yield event([{"thoughtSignature": "body-signature"}])
                            yield event([{"text": answer}], "STOP")
                        self.saved.reset_mock()
                        with ExitStack() as stack:
                            stack.enter_context(patch.object(api, "stream_post_async", upstream))
                            stack.enter_context(patch("config.get_return_thoughts_to_frontend", AsyncMock(return_value=include)))
                            for module in (gemini, openai, anthropic):
                                stack.enter_context(patch.object(module, "is_anti_truncation_model", return_value=anti))
                                stack.enter_context(patch.object(module, "get_anti_truncation_max_attempts", AsyncMock(return_value=1)))
                            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                                response = await client.post(url, json=body)
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(len(calls), 1)
                        reasoning, content = "", ""
                        for line in response.text.splitlines():
                            if not line.startswith("data:") or line[5:].strip() == "[DONE]":
                                continue
                            value = json.loads(line[5:]); value = value.get("response", value)
                            self.assertNotIn("error", value)
                            if protocol == "gemini":
                                for candidate in value.get("candidates", []):
                                    for part in candidate.get("content", {}).get("parts", []):
                                        if part.get("thought"):
                                            reasoning += part.get("text", "")
                                        else:
                                            content += part.get("text", "")
                            elif protocol == "openai":
                                for choice in value.get("choices", []):
                                    reasoning += choice.get("delta", {}).get("reasoning_content") or ""
                                    content += choice.get("delta", {}).get("content") or ""
                            elif value.get("type") == "content_block_delta":
                                reasoning += value["delta"].get("thinking", "")
                                content += value["delta"].get("text", "")
                        self.assertEqual(reasoning, "native reasoning")
                        self.assertEqual(content, answer)
                        self.assertEqual(self.saved.await_count, 1)
                        self.assertTrue(self.saved.call_args.args[0]["success"])
                        self.assertEqual(self.saved.call_args.args[0]["request_id"], response.headers["x-request-id"])

    async def test_default_stream_preserves_user_system_signature_and_tool(self):
        request = {"contents": [{"role": "user", "parts": [{"text": "test"}]}],
                   "systemInstruction": {"parts": [{"text": "user rules"}]}}
        before = copy.deepcopy(request)
        parts = [{"text": "native reasoning", "thought": True, "thoughtSignature": "thought-signature"},
                 {"text": "answer"}, {"functionCall": {"name": "lookup", "args": {"q": "test"}},
                                        "thoughtSignature": "tool-signature"}]
        async def upstream(**kwargs):
            self.assertEqual(kwargs["body"]["request"]["systemInstruction"], request["systemInstruction"])
            yield event(parts, "STOP")
        with patch.object(api, "stream_post_async", upstream):
            chunks = [c async for c in api.stream_request({"model": "gemini-4-argon-medium", "request": request})]
        self.assertEqual(request, before)
        value = json.loads(chunks[0][5:])["response"]
        self.assertEqual(value["candidates"][0]["content"]["parts"], parts)
        self.assertEqual(value["usageMetadata"]["totalTokenCount"], 123)
        self.assertTrue(self.saved.call_args.args[0]["success"])

    async def test_native_only_mode_does_not_reclassify_unmarked_prose(self):
        answer = "Thinking Process:\n1. **Check:** quoted text.\n这是用户要求的原文。"
        async def upstream(**kwargs):
            yield event([{"text": answer}], "STOP")
        with patch.object(api, "stream_post_async", upstream), patch.dict(
                os.environ, {"ARGON_PASSIVE_THOUGHT_RECOVERY_ENABLED": "false"}):
            chunks = [c async for c in api.stream_request({"model": "gemini-4-argon-medium", "request": {"contents": []}})]
        self.assertEqual(text_of(chunks), answer)
        self.assertEqual(text_of(chunks, True), "")

    async def test_default_incomplete_stream_is_not_success(self):
        async def upstream(**kwargs):
            yield event([{"text": "partial answer"}])
        with patch.object(api, "stream_post_async", upstream):
            chunks = [c async for c in api.stream_request({"model": "gemini-4-argon-medium", "request": {"contents": []}})]
        self.assertIsInstance(chunks[-1], Response)
        self.assertIn("UPSTREAM_INCOMPLETE_STREAM", chunks[-1].body.decode())
        self.assertFalse(self.saved.call_args.args[0]["success"])

    async def test_native_thought_only_completion_does_not_trigger_empty_retry(self):
        async def upstream(**kwargs):
            yield event([{"text": "hidden", "thought": True}], "STOP")
        with patch.object(api, "stream_post_async", upstream):
            chunks = [c async for c in api.stream_request({"model": "gemini-4-argon-medium", "request": {
                "contents": [], "generationConfig": {"thinkingConfig": {"includeThoughts": False}}}})]
        self.assertEqual(text_of(chunks, True), "hidden")
        self.assertEqual(text_of(chunks), "")
        self.assertFalse(any(isinstance(c, Response) for c in chunks))
        self.assertEqual(self.saved.await_count, 1)
        self.assertTrue(self.saved.call_args.args[0]["success"])

    async def test_optout_accepts_usage_only_metadata_with_null_candidates(self):
        async def upstream(**kwargs):
            yield event([{"text": "answer"}])
            yield 'data: {"response": {"candidates": null, "usageMetadata": {"totalTokenCount": 99}}}\n\n'
            yield "data: [DONE]\n\n"
        with patch.object(api, "stream_post_async", upstream):
            chunks = [c async for c in api.stream_request({"model": "gemini-4-argon-medium", "request": {
                "contents": [], "generationConfig": {"thinkingConfig": {"includeThoughts": False}}}})]
        self.assertFalse(any(isinstance(c, Response) for c in chunks))
        self.assertIn('"totalTokenCount": 99', "".join(chunks))
        self.assertIn("[DONE]", chunks[-1])
        self.assertTrue(self.saved.call_args.args[0]["success"])

    async def test_native_optout_preserves_partial_body_and_upstream_error(self):
        for body_text in ("", "partial answer"):
            with self.subTest(body=bool(body_text)):
                parts = [{"text": "hidden", "thought": True, "thoughtSignature": "hidden-signature"}]
                if body_text:
                    parts.append({"text": body_text, "thoughtSignature": "body-signature"})
                async def upstream(**kwargs):
                    yield event(parts, "PROHIBITED_CONTENT")
                self.saved.reset_mock()
                with patch.object(api, "stream_post_async", upstream):
                    chunks = [c async for c in api.stream_request({"model": "gemini-4-argon-medium", "request": {
                        "contents": [], "generationConfig": {"thinkingConfig": {"includeThoughts": False}}}})]
                self.assertEqual(text_of(chunks), body_text)
                self.assertEqual(text_of(chunks, True), "hidden")
                self.assertIn("hidden-signature", str(chunks))
                self.assertIsInstance(chunks[-1], Response)
                self.assertEqual(chunks[-1].status_code, 400)
                self.assertIn("PROHIBITED_CONTENT", chunks[-1].body.decode())
                if body_text:
                    self.assertIn("body-signature", str(chunks))
                else:
                    self.assertEqual(len(chunks), 2)
                self.assertEqual(self.saved.await_count, 1)
                self.assertFalse(self.saved.call_args.args[0]["success"])
