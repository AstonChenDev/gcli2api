import json

import pytest

from src.router.hi_check import greeting_probe_response, is_health_check_request


@pytest.mark.parametrize("extra_parts", [
    [{"text": "Translate the greeting."}],
    [{"inlineData": {"mimeType": "image/png", "data": "YQ=="}}],
])
def test_gemini_multipart_greeting_is_a_generation_request(extra_parts):
    body = {"contents": [{"role": "user", "parts": [{"text": "Hi"}, *extra_parts]}]}
    assert not is_health_check_request(body, "gemini")


def test_empty_gemini_parts_do_not_trigger_health_check():
    assert not is_health_check_request({"contents": [{"role": "user", "parts": []}]}, "gemini")


@pytest.mark.parametrize(("protocol", "context"), [
    ("gemini", {"systemInstruction": {"parts": [{"text": "Translate the greeting."}]}}),
    ("gemini", {"generationConfig": {"responseMimeType": "application/json"}}),
    ("gemini", {"generationConfig": {"responseJsonSchema": {}}}),
    ("gemini", {"cachedContent": "cachedContents/test"}),
    ("gemini", {"cached_content": "cachedContents/test"}),
    ("gemini", {"generationConfig": {"responseModalities": ["AUDIO"]}}),
    ("gemini", {"generationConfig": {"thinkingConfig": {"thinkingLevel": "high"}}}),
    ("openai", {"reasoning_effort": "high"}),
    ("openai", {"reasoning_effort": ""}),
    ("openai", {"reasoning": {"effort": "low"}}),
    ("anthropic", {"output_config": {"effort": "medium"}}),
    ("anthropic", {"thinking": {"type": "enabled", "budget_tokens": 1024}}),
    ("anthropic", {"system": "Translate the greeting."}),
    ("openai", {"tools": [{"type": "function", "function": {"name": "translate"}}]}),
])
def test_plain_greeting_probe_and_request_with_context(protocol, context):
    if protocol == "gemini":
        body = {"contents": [{"role": "user", "parts": [{"text": "Hi"}]}]}
    else:
        body = {"messages": [{"role": "user", "content": "Hi"}]}
    assert is_health_check_request(body, protocol)
    assert not is_health_check_request({**body, **context}, protocol)


@pytest.mark.parametrize("protocol", ["openai", "anthropic"])
def test_streaming_probe_delivers_equivalent_message_and_terminal_event(protocol):
    plain = json.loads(greeting_probe_response(protocol, False).body)
    response = greeting_probe_response(protocol, True)
    assert response.media_type == "text/event-stream"
    records = response.body.decode().strip().split("\n\n")
    if protocol == "openai":
        assert records.pop() == "data: [DONE]"
        chunks = [json.loads(record.removeprefix("data: ")) for record in records]
        assert all(chunk["object"] == "chat.completion.chunk" and chunk["id"] == plain["id"] for chunk in chunks)
        first, last = [chunk["choices"][0] for chunk in chunks]
        assert first["delta"] == plain["choices"][0]["message"]
        assert first["finish_reason"] is None
        assert last == {"delta": {}, "finish_reason": "stop", "index": 0}
    else:
        events = []
        for record in records:
            name, data = record.split("\n")
            event = json.loads(data.removeprefix("data: "))
            assert name == "event: " + event["type"]
            events.append(event)
        assert [event["type"] for event in events] == [
            "message_start", "content_block_start", "content_block_delta",
            "content_block_stop", "message_delta", "message_stop",
        ]
        assert events[0]["message"]["id"] == plain["id"]
        assert events[0]["message"]["stop_reason"] is None
        assert [event["index"] for event in events[1:4]] == [0, 0, 0]
        assert events[2]["delta"]["text"] == plain["content"][0]["text"]
        assert events[4]["delta"]["stop_reason"] == plain["stop_reason"]
        assert events[4]["usage"]["output_tokens"] == 0
