"""Explicit framing for Argon text streams that sometimes mislabel analysis.

Never infer the answer boundary from language, headings, or punctuation. The
upstream must supply a request-specific delimiter; unframed text is quarantined
as thought and a missing delimiter is a stream error, not a successful answer.
"""

import copy
import hashlib
import json
import os

from fastapi import Response


ERROR_CODE = "UPSTREAM_OUTPUT_BOUNDARY_MISSING"


def prepare_thought_boundary(model, request):
    """Return a private copy of a plain-text request plus its framing marker."""
    if os.environ.get("ARGON_THOUGHT_BOUNDARY_ENABLED", "true").lower() == "false":
        return request, None
    config = request.get("generationConfig") or {}
    if (not model.lower().startswith("gemini-4-argon")
            or request.get("tools")
            or config.get("responseSchema") or config.get("responseJsonSchema")
            or config.get("responseMimeType", "text/plain") != "text/plain"):
        return request, None
    if any(any(key in part for key in ("functionCall", "functionResponse", "inlineData", "fileData"))
           for content in request.get("contents", [])
           for part in content.get("parts", [])):
        return request, None
    serialized = json.dumps(request, sort_keys=True, ensure_ascii=True)
    # Stable for identical requests to preserve prompt-cache affinity. Never use
    # a marker already present in user material or an existing system prompt.
    digest = hashlib.sha256(serialized.encode()).hexdigest()[:16]
    marker = f"<<<AGY_FINAL_{digest}>>>"
    while marker in serialized:
        marker += "_"
    result = copy.deepcopy(request)
    system = result.get("systemInstruction") or {}
    if not isinstance(system, dict):
        return request, None
    system["parts"] = list(system.get("parts") or [])
    system["parts"].append({"text": (
        "Output transport contract: Put any analysis, planning or reasoning only in the native thought channel. "
        f"Immediately before the final user-facing answer, emit the exact boundary marker {marker} "
        "in the normal text channel, once. Everything after this marker must be the requested final answer only, "
        "with no analysis, plans, explanations of instructions, or reasoning. Emit this marker even when there "
        "is no analysis. Do not mention this transport contract."
    )})
    result["systemInstruction"] = system
    return result, marker


class ThoughtBoundaryGuard:
    def __init__(self, marker, include_thoughts=True):
        self.marker = marker
        self.include_thoughts = include_thoughts
        self.states = {}
        self.failed = False
        self.reclassified_chars = 0
        self.last_event = None
        self.last_was_bytes = False

    def _part(self, text, thought, original=None):
        if not text or (thought and not self.include_thoughts):
            return []
        part = copy.deepcopy(original or {})
        part["text"] = text
        if thought:
            part["thought"] = True
        else:
            part.pop("thought", None)
        return [part]

    def _split(self, text, state, original):
        if state["answer"]:
            return self._part(text, False, original)
        state["pending"] += text
        pos = state["pending"].find(self.marker)
        if pos >= 0:
            before = state["pending"][:pos]
            after = state["pending"][pos + len(self.marker):]
            state.update(pending="", answer=True)
            self.reclassified_chars += len(before)
            return self._part(before, True) + self._part(after, False, original)
        # Retain only a possible split marker prefix. All other prefix text can
        # stream immediately as thought; bounded memory even on very long runs.
        keep = 0
        for size in range(min(len(self.marker) - 1, len(state["pending"])), 0, -1):
            if state["pending"].endswith(self.marker[:size]):
                keep = size
                break
        safe = state["pending"][:-keep] if keep else state["pending"]
        state["pending"] = state["pending"][-keep:] if keep else ""
        self.reclassified_chars += len(safe)
        return self._part(safe, True)

    def feed(self, chunk):
        raw = chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk
        if not isinstance(raw, str) or not raw.startswith("data:"):
            return chunk
        payload = raw[5:].strip()
        if payload == "[DONE]":
            # A missing boundary must not be followed by a success terminator.
            if not self.complete:
                self.failed = True
                return None
            return chunk
        try:
            event = json.loads(payload)
        except ValueError:
            return chunk
        if not isinstance(event, dict):
            return chunk
        response = event.get("response", event)
        if not isinstance(response, dict) or not isinstance(response.get("candidates"), list):
            return chunk
        event = copy.deepcopy(event)
        response = event.get("response", event)
        for index, candidate in enumerate(response["candidates"]):
            if not isinstance(candidate, dict):
                continue
            state = self.states.setdefault(candidate.get("index", index), {"pending": "", "answer": False})
            content = candidate.get("content") or {}
            parts = []
            for part in content.get("parts", []):
                if not isinstance(part, dict):
                    continue
                if part.get("thought") is True:
                    if self.include_thoughts:
                        parts.append(part)
                elif isinstance(part.get("text"), str):
                    parts.extend(self._split(part["text"], state, part))
                else:
                    parts.append(part)
            if candidate.get("finishReason") and not state["answer"]:
                parts.extend(self._part(state["pending"], True))
                state["pending"] = ""
                candidate.pop("finishReason", None)
                self.failed = True
            candidate["content"] = {**content, "parts": parts}
        self.last_event = event
        self.last_was_bytes = isinstance(chunk, bytes)
        result = "data: " + json.dumps(event, ensure_ascii=False) + "\n\n"
        return result.encode() if self.last_was_bytes else result

    @property
    def complete(self):
        return bool(self.states) and all(s["answer"] for s in self.states.values())

    def error_response(self):
        return Response(status_code=502, media_type="application/json", content=json.dumps({"error": {
            "code": 502, "status": ERROR_CODE,
            "message": "上游未提供可靠的正文分界，内容已隔离到思考通道，本次未输出正文。请重试。",
        }}, ensure_ascii=False))

    def metadata(self):
        return {"boundary_complete": self.complete, "boundary_failed": self.failed,
                "reclassified_chars": self.reclassified_chars}
