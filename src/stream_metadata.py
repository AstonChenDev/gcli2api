"""Bounded, content-free metadata for tracing upstream response IDs."""
import json


class StreamMetadata:
    def __init__(self):
        self.response_id = ""
        self.model_version = ""
        self.thought_parts = 0
        self.text_parts = 0

    def observe(self, chunk):
        if isinstance(chunk, bytes):
            chunk = chunk.decode("utf-8", errors="replace")
        if not isinstance(chunk, str) or not chunk.startswith("data: "):
            return
        try:
            data = json.loads(chunk[6:])
        except (ValueError, TypeError):
            return
        if not isinstance(data, dict):
            return
        data = data.get("response", data)
        if not isinstance(data, dict):
            return
        for key, attr in (("responseId", "response_id"), ("modelVersion", "model_version")):
            value = data.get(key)
            if isinstance(value, str):
                setattr(self, attr, value[:160])
        candidates = data.get("candidates")
        if not isinstance(candidates, list):
            return
        for candidate in candidates:
            content = candidate.get("content") if isinstance(candidate, dict) else None
            parts = content.get("parts") if isinstance(content, dict) else None
            if not isinstance(parts, list):
                continue
            for part in parts:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    if part.get("thought") is True:
                        self.thought_parts += 1
                    else:
                        self.text_parts += 1

    def as_dict(self):
        return dict(response_id=self.response_id, model_version=self.model_version,
                    thought_parts=self.thought_parts, text_parts=self.text_parts)
