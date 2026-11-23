"""Bounded, content-free metadata for tracing upstream response IDs."""
import json
import time


BLOCK_REASONS = {
    "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII",
    "IMAGE_SAFETY", "IMAGE_PROHIBITED_CONTENT", "IMAGE_RECITATION",
}


_TOKEN_FIELDS = {
    "promptTokenCount": ("promptTokenCount", "total_input_tokens", "input_tokens"),
    "candidatesTokenCount": ("candidatesTokenCount", "total_output_tokens"),
    "thoughtsTokenCount": ("thoughtsTokenCount", "total_thought_tokens"),
    "totalTokenCount": ("totalTokenCount", "total_tokens"),
    "cachedContentTokenCount": ("cachedContentTokenCount", "cached_content_token_count", "total_cached_tokens",
                                "cached_tokens", "cache_read_input_tokens"),
}


def token_counters(metadata):
    """Read reported counters only. An omitted counter differs from an explicit zero."""
    if not isinstance(metadata, dict):
        return {}

    def first_int(source, keys):
        for key in keys:
            value = source.get(key)
            if value is not None and not isinstance(value, bool):
                try:
                    return max(0, int(value))
                except (TypeError, ValueError, OverflowError):
                    continue
        return None

    counts = {}
    for field, aliases in _TOKEN_FIELDS.items():
        value = first_int(metadata, aliases)
        if value is not None:
            counts[field] = value
    if "cachedContentTokenCount" not in counts:
        for key in ("prompt_tokens_details", "input_tokens_details"):
            details = metadata.get(key)
            if isinstance(details, dict):
                cached = first_int(details, ("cached_tokens",))
                if cached is not None:
                    counts["cachedContentTokenCount"] = cached
                    break
    return counts


class StreamMetadata:
    def __init__(self):
        self.response_id = ""
        self.model_version = ""
        self.thought_parts = 0
        self.text_parts = 0
        self.last_event_at = None

    def observe(self, chunk):
        if isinstance(chunk, bytes):
            chunk = chunk.decode("utf-8", errors="replace")
        if not isinstance(chunk, str) or not chunk.startswith("data:"):
            return
        self.last_event_at = time.time()
        try:
            data = json.loads(chunk[5:].strip())
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
                    thought_parts=self.thought_parts, text_parts=self.text_parts,
                    upstream_last_event_at=self.last_event_at)


def part_has_output(part):
    return bool(part.get("text")) or any(key in part for key in (
        "functionCall", "inlineData", "fileData", "executableCode", "codeExecutionResult"
    ))


class StreamCompletion:
    """Completion of protocol events handed to the downstream converter.

    A body boundary only marks the start of the answer. It is not completion.
    No response text is retained here.
    """
    def __init__(self, expected_candidates=1):
        try:
            self.expected_candidates = max(1, int(expected_candidates or 1))
        except (ValueError, TypeError, OverflowError):
            self.expected_candidates = 1
        self.candidates = set()
        self.finish_reasons = {}
        self.done = False
        self.error = False
        self.eof = False

    def observe(self, chunk):
        if isinstance(chunk, bytes):
            chunk = chunk.decode('utf-8', errors='replace')
        if not isinstance(chunk, str) or not chunk.startswith('data:'):
            return
        payload = chunk[5:].strip()
        if payload == '[DONE]':
            self.done = True
            return
        try:
            data = json.loads(payload)
        except (ValueError, TypeError):
            return
        if not isinstance(data, dict):
            return
        data = data.get('response', data)
        if not isinstance(data, dict):
            return
        if data.get('error'):
            self.error = True
        candidates = data.get('candidates')
        if not isinstance(candidates, list):
            return
        for position, candidate in enumerate(candidates):
            if not isinstance(candidate, dict):
                continue
            index = candidate.get('index', position)
            if not isinstance(index, int) or index < 0 or index >= 128:
                continue
            reason = candidate.get('finishReason')
            if isinstance(reason, str) and reason and reason != 'FINISH_REASON_UNSPECIFIED':
                self.finish_reasons[index] = reason[:80]
            else:
                content = candidate.get('content')
                parts = content.get('parts') if isinstance(content, dict) else None
                if not isinstance(parts, list) or not any(isinstance(p, dict) and part_has_output(p) for p in parts):
                    continue
            self.candidates.add(index)

    @property
    def complete(self):
        if self.error:
            return False
        return (len(self.candidates) >= self.expected_candidates and
            (self.done or self.candidates.issubset(self.finish_reasons)))

    def as_dict(self):
        return {'stream_complete': self.complete, 'upstream_eof': self.eof,
                'done_seen': self.done, 'finish_reasons': self.finish_reasons}
