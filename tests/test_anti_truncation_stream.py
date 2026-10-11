import json

import pytest
from fastapi.responses import StreamingResponse

from src.converter.anti_truncation import AntiTruncationStreamProcessor


def sse_chunk(parts, finish_reason=None):
    candidate = {"content": {"role": "model", "parts": parts}, "index": 0}
    if finish_reason:
        candidate["finishReason"] = finish_reason
    return ("data: " + json.dumps({"candidates": [candidate]}) + "\n\n").encode()


def stream_candidates(chunks):
    return [
        candidate
        for chunk in chunks
        if chunk.startswith(b"data: ") and chunk.strip() != b"data: [DONE]"
        for candidate in json.loads(chunk[6:]).get("candidates", [])
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("finish_with_text", [False, True])
async def test_plain_text_is_delivered_before_upstream_finishes(finish_with_text):
    events = []

    async def body():
        events.append("first")
        yield sse_chunk([{"text": "hello"}], "STOP" if finish_with_text else None)
        events.append("finished")
        if not finish_with_text:
            yield sse_chunk([], "STOP")
        yield b"data: [DONE]\n\n"

    async def request(payload):
        events.append("request")
        return StreamingResponse(body())

    processor = AntiTruncationStreamProcessor(request, {"request": {"contents": []}})
    stream = processor.process_stream()
    first = await anext(stream)

    assert events == ["request", "first"]
    assert "finishReason" not in json.loads(first[6:])["candidates"][0]
    remaining = [chunk async for chunk in stream]
    candidates = stream_candidates([first, *remaining])
    assert events.count("request") == 1
    assert "".join(
        part.get("text", "")
        for candidate in candidates
        for part in candidate["content"]["parts"]
    ) == "hello"
    assert [c["finishReason"] for c in candidates if "finishReason" in c] == ["STOP"]
    assert remaining[-1] == b"data: [DONE]\n\n"


@pytest.mark.asyncio
async def test_interrupted_text_is_not_repeated_when_retries_are_exhausted():
    async def body():
        yield sse_chunk([{"text": "partial"}])
        raise RuntimeError("upstream disconnected")

    async def request(payload):
        return StreamingResponse(body())

    processor = AntiTruncationStreamProcessor(request, {"request": {}}, max_attempts=1)
    chunks = [chunk async for chunk in processor.process_stream()]
    candidates = stream_candidates(chunks)

    assert "".join(
        part.get("text", "")
        for candidate in candidates
        for part in candidate["content"]["parts"]
    ) == "partial"
    assert [c["finishReason"] for c in candidates if "finishReason" in c] == ["STOP"]
    assert chunks[-1] == b"data: [DONE]\n\n"


@pytest.mark.asyncio
@pytest.mark.parametrize("finish_reason", [None, "STOP"])
async def test_synthetic_answer_has_one_finish_signal(finish_reason):
    async def body():
        yield sse_chunk(
            [{"functionCall": {"name": "emit_answer", "args": {"content": "answer"}}}],
            finish_reason,
        )
        yield b"data: [DONE]\n\n"

    async def request(payload):
        return StreamingResponse(body())

    processor = AntiTruncationStreamProcessor(request, {"request": {}})
    chunks = [chunk async for chunk in processor.process_stream()]
    candidates = stream_candidates(chunks)

    assert "".join(
        part.get("text", "")
        for candidate in candidates
        for part in candidate["content"]["parts"]
    ) == "answer"
    assert [c["finishReason"] for c in candidates if "finishReason" in c] == ["STOP"]
    assert chunks[-1] == b"data: [DONE]\n\n"
