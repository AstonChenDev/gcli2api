"""Classify upstream SSE lines without guessing whether text is reasoning.

The transport separates upstream lines at SSE line endings.
Content classification here never changes thought flags or thought signatures.
"""
import json
import copy
from fastapi import Response
from src.stream_metadata import BLOCK_REASONS, part_has_output, token_counters


# Observed verbatim on the upstream wire, without error/usage/finish fields.
# Do not infer a rejection from arbitrary prose containing policy keywords.
PROMPT_REJECTION_NOTICE = (
    "The prompt could not be submitted. The prompt contains sensitive words that violate Google's "
    "[Generative AI Prohibited Use policy](https://policies.google.com/terms/generative-ai/use-policy). "
    "Try rephrasing the prompt. If you think this was an error, "
    "[send feedback](https://ai.google.dev/gemini-api/docs/troubleshooting)."
)


def prompt_rejection_notice(response):
    if set(response) != {"candidates"}:
        return None
    candidates = response["candidates"]
    if not isinstance(candidates, list) or len(candidates) != 1:
        return None
    candidate = candidates[0]
    if not isinstance(candidate, dict) or set(candidate) != {"content"}:
        return None
    content = candidate["content"]
    if not isinstance(content, dict) or set(content) != {"parts"}:
        return None
    parts = content["parts"]
    if not isinstance(parts, list) or len(parts) != 1 or not isinstance(parts[0], dict):
        return None
    if set(parts[0]) == {"text"} and parts[0]["text"] == PROMPT_REJECTION_NOTICE:
        return PROMPT_REJECTION_NOTICE
    return None


def stream_error(status, code, message, **details):
    return Response(status_code=status, media_type="application/json", content=json.dumps(
        {"error": {"code": status, "status": code, "message": message, **details}},
        ensure_ascii=False))


def _with_partial_content(error, event):
    """Keep valid same-event output without promoting the error to a successful finish."""
    partial = copy.deepcopy(event)
    inner = partial.get("response", partial)
    inner.pop("error", None)
    inner.pop("promptFeedback", None)
    candidates = inner.get("candidates")
    if not isinstance(candidates, list):
        return error
    for candidate in candidates:
        if isinstance(candidate, dict):
            candidate.pop("finishReason", None)
    # Reuse the shape/content checks; the error and block fields are removed,
    # so this validation cannot recursively enter another error branch.
    kind, line = inspect_line("data: " + json.dumps(partial, ensure_ascii=False))
    if kind == "content":
        error.upstream_content = line
    return error


def gemini_metadata_frames(response, seen_identity):
    """Keep public response and candidate metadata seen before the first output."""
    prefix = {}
    for key in ("responseId", "modelVersion", "createTime"):
        value = response.get(key)
        if value is not None and value != seen_identity.get(key):
            prefix[key] = seen_identity[key] = value
    candidates = []
    for index, candidate in enumerate(response.get("candidates") or []):
        # Content/signatures and finish reasons have separate delivery rules.
        metadata = {k: v for k, v in candidate.items() if k not in ("index", "content", "finishReason", "finishMessage")}
        if metadata:
            candidates.append({**metadata, "index": candidate.get("index", index)})
    if candidates:
        prefix["candidates"] = candidates
    if prefix:
        yield "data: " + json.dumps(prefix, ensure_ascii=False) + "\n\n"

    details = {}
    feedback = response.get("promptFeedback")
    if isinstance(feedback, dict) and feedback:
        details["promptFeedback"] = feedback
    usage = response.get("usageMetadata")
    # StreamUsage carries numeric counters forward, but per-modality breakdowns
    # and other native fields belong to the original usage snapshot.
    if isinstance(usage, dict) and usage.keys() - token_counters(usage).keys():
        details["usageMetadata"] = usage
    if details:
        yield "data: " + json.dumps(details, ensure_ascii=False) + "\n\n"


class StreamUsage:
    """Carry cumulative Gemini token counters across events of one attempt."""

    def __init__(self):
        self.counts = {}
        self.started = False

    def observe(self, reported, envelope=None):
        current = token_counters(reported)
        if isinstance(envelope, dict):
            # As with non-stream accounting, explicit envelope counters take precedence.
            current.update(token_counters(envelope))
        # A previous total may predate a new input or output count.
        components = ("promptTokenCount", "candidatesTokenCount", "thoughtsTokenCount")
        if any(key in current and current[key] != self.counts.get(key) for key in components):
            self.counts.pop("totalTokenCount", None)
        self.counts.update(current)
        return current

    def apply(self, response, has_content, current):
        first_content = has_content and not self.started
        self.started |= has_content
        reported = response.get("usageMetadata")
        if not isinstance(reported, dict):
            if not first_content and not any(c.get("finishReason") for c in response.get("candidates") or []):
                return
            reported = {}
        missing = self.counts.keys() - current.keys()
        if missing:
            # Null or unreadable fields do not replace an earlier reported count.
            response["usageMetadata"] = {**reported, **{key: self.counts[key] for key in missing}}


async def finish_data_record(first_line, lines):
    """Join the data fields of a split JSON object up to its event boundary."""
    fields = [first_line[5:]]
    async for line in lines:
        if not isinstance(line, str):
            return first_line
        if line == "":
            break
        if line.startswith("data:"):
            fields.append(line[5:].removeprefix(" "))
        elif line == "data":
            fields.append("")
        # Other SSE fields and comments are not part of the data payload.
    return "data: " + "\n".join(fields)


class CandidateBlocks:
    """Hold candidate-level refusals while a requested sibling can still answer."""

    def __init__(self, expected, *, streaming=True):
        self.expected = expected
        self.streaming = streaming
        self.indices = set()
        self.output_indices = set()
        self.error = None

    @property
    def unanswered_error(self):
        return None if self.output_indices - self.indices else self.error

    def note_output(self, index):
        if type(index) is int and 0 <= index < self.expected:
            self.output_indices.add(index)

    def defer(self, indices, error):
        if not all(type(i) is int and 0 <= i < self.expected for i in indices):
            return False
        self.indices.update(indices)
        self.error = error
        return len(self.indices) < self.expected and (self.streaming or self.unanswered_error is None)


def inspect_line(chunk, *, allow_text_notice=False, allow_fragment=False, usage=None, candidate_blocks=None):
    """Return (kind, normalized line or Response); no response text is retained."""
    try:
        raw = chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk
    except UnicodeDecodeError:
        return "invalid", stream_error(500, "UPSTREAM_INVALID_EVENT", "上游流事件不是有效 UTF-8")
    if not isinstance(raw, str) or not raw.strip():
        return "empty", None
    if raw.startswith(":"):
        return "heartbeat", raw.rstrip("\r\n") + "\n\n"
    # SSE permits a field with an empty value to omit the colon.
    if raw in ("event", "id", "retry") or raw.startswith(("event:", "id:", "retry:")):
        return "empty", None
    physical_line = allow_fragment and isinstance(chunk, str) and not raw.endswith(("\r", "\n"))
    if physical_line and raw == "data":
        return "blank_data", raw
    if not raw.startswith("data:"):
        return "invalid", stream_error(500, "UPSTREAM_INVALID_EVENT", "上游返回了无效的流事件")
    payload = raw[5:].strip()
    if physical_line and not payload:
        return "blank_data", raw
    if payload == "[DONE]":
        return "done", "data: [DONE]\n\n"
    try:
        event = json.loads(payload)
    except json.JSONDecodeError as exc:
        # A JSON object can continue on a new data field only at a token
        # boundary. A syntax error inside the line is already conclusive.
        if physical_line and payload.startswith("{") and exc.pos == len(payload):
            return "fragment", chunk
        event = None
    except ValueError:
        event = None
    response = event.get("response", event) if isinstance(event, dict) else None
    # Capture reported usage before early returns for rejections or malformed
    # candidates. Keep accounting scoped to this upstream attempt.
    current_usage = {}
    if usage is not None and isinstance(event, dict):
        reported = response.get("usageMetadata") if isinstance(response, dict) else None
        envelope = event.get("usageMetadata") if response is not event else None
        if isinstance(reported, dict) or isinstance(envelope, dict):
            current_usage = usage.observe(reported, envelope)
    if not isinstance(response, dict):
        return "invalid", stream_error(500, "UPSTREAM_INVALID_EVENT", "上游返回了无效的 JSON 流事件")
    notice = prompt_rejection_notice(response) if allow_text_notice else None
    if notice:
        return "blocked", stream_error(400, "UPSTREAM_CONTENT_BLOCKED", notice,
            reasonSource="upstream_text_notice", upstreamHttpStatus=200)
    if response.get("error"):
        error = response["error"]
        code = error.get("code") if isinstance(error, dict) else None
        status = code if isinstance(code, int) and not isinstance(code, bool) and 400 <= code <= 599 else 500
        result = Response(status_code=status, media_type="application/json",
                          content=json.dumps({"error": error}, ensure_ascii=False))
        return "error", _with_partial_content(result, event)
    feedback = response.get("promptFeedback") or {}
    blocked = feedback.get("blockReason") if isinstance(feedback, dict) else None
    if blocked == "BLOCK_REASON_UNSPECIFIED":
        blocked = None
    candidates = response.get("candidates") or []
    if not isinstance(candidates, list):
        return "invalid", stream_error(500, "UPSTREAM_INVALID_EVENT", "上游 candidates 格式无效")
    prompt_block = blocked
    blocked_indices = []
    has_content = False
    for position, candidate in enumerate(candidates):
        if not isinstance(candidate, dict):
            return "invalid", stream_error(500, "UPSTREAM_INVALID_EVENT", "上游 candidate 格式无效")
        reason = candidate.get("finishReason")
        content = candidate.get("content") or {}
        parts = content.get("parts", []) if isinstance(content, dict) else None
        if not isinstance(parts, list) or any(not isinstance(p, dict) for p in parts):
            return "invalid", stream_error(500, "UPSTREAM_INVALID_EVENT", "上游 parts 格式无效")
        output = any(part_has_output(p) for p in parts)
        has_content |= output
        index = candidate.get("index", position)
        if output and candidate_blocks is not None:
            candidate_blocks.note_output(index)
        if isinstance(reason, str) and reason in BLOCK_REASONS:
            blocked = reason
            blocked_indices.append(index)
    if blocked:
        error = stream_error(400, "UPSTREAM_CONTENT_BLOCKED", f"上游拦截了本次生成: {blocked}",
                             blockReason=blocked, promptFeedback=feedback,
                             finishReasons=[c.get("finishReason") for c in candidates if c.get("finishReason")])
        # Content and a block reason may arrive together. Preserve the partial
        # content before the error, without forwarding a successful terminator.
        if (prompt_block or candidate_blocks is None
                or not candidate_blocks.defer(blocked_indices, error)):
            if has_content:
                _with_partial_content(error, event)
            return "blocked", error
    if usage is not None:
        usage.apply(response, has_content, current_usage)
    return ("content" if has_content else "metadata"), "data: " + json.dumps(event, ensure_ascii=False) + "\n\n"


def error_payload(response):
    """Retain structured error details across protocol adapters."""
    try:
        payload = json.loads(response.body)
        if isinstance(payload, dict) and "error" in payload:
            return payload
        message = payload
    except (ValueError, TypeError):
        message = response.body.decode("utf-8", errors="replace")
    return {"error": {"code": response.status_code, "message": str(message)}}
