import json
import unittest
from fastapi import Response
from src.converter.anthropic2gemini import gemini_stream_to_anthropic_stream


class ThoughtErrorConversionTests(unittest.IsolatedAsyncioTestCase):
    async def test_anthropic_blocked_error_is_a_real_error_event(self):
        async def source():
            yield Response(status_code=400, content=json.dumps({"error": {"code": 400, "status": "UPSTREAM_CONTENT_BLOCKED", "message": "PROHIBITED_CONTENT"}}))
        output = [x async for x in gemini_stream_to_anthropic_stream(source(), "gemini-4-argon-medium")]
        self.assertTrue(output[0].startswith(b"event: error\n"))
        self.assertIn(b"PROHIBITED_CONTENT", output[0])
