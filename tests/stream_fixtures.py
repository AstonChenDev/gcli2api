import json
from fastapi import Response


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
