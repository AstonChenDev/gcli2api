"""Preserve native thoughts and recover mislabeled planning without requiring a marker."""

import copy
import hashlib
import json
import os
import re
from collections import deque

from src.converter.request_fields import normalize_gemini_request_fields


ERROR_CODE = "UPSTREAM_OUTPUT_BOUNDARY_MISSING"
POLLUTION_ERROR_CODE = "UPSTREAM_OUTPUT_BOUNDARY_INVALID"


class ThoughtBoundaryError(ValueError):
    """The private framing contract was violated; text cannot be trusted."""


class _FramingProblem(ValueError):
    """A framing fault that may be resynchronized before the answer starts."""


class _NativePart(dict):
    """A queued native part; its text is independent of undecided plain text."""


def prepare_thought_boundary(model, request):
    """Return a private copy of a plain-text request plus its framing marker."""
    request = normalize_gemini_request_fields(request)
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
    """Incremental framing, with conservative recovery for leaked planning headings.

    The request marker and narrowly recognized copy errors define framing.
    The unmarked narrative fallback is a
    heuristic, restricted to structured English planning followed by a complete
    Chinese prose sentence. Malformed framing before the answer waits for an
    exact standalone marker on the same stream. Unresolved anomalies fail;
    no new generation or language-based body guessing is used for recovery.
    """
    RECOVERY_LIMIT = 16384
    REPEAT_LIMIT = 32
    RECOVERY_PART_LIMIT = 256
    RECOVERY_METADATA_LIMIT = 65536
    START_LIMIT = 128
    LOOKBEHIND = 1024
    HEADER = re.compile(r"^\s*(?:#{1,6}\s*)?(?:\*\*|__)?Thinking Process(?:\*\*|__)?\s*[:：](?:\*\*|__)?[ \t]*(?:\r?\n|$)", re.I)
    SECTION = re.compile(r"(?:^|\n)\s*(\d+)\.\s+\*\*[A-Za-z]")
    PROSE = re.compile(r"(?<=[.])([ \t\r\n]*)([\u4e00-\u9fff][^\n。！？]{0,512}[。！？])")
    PLANNING_ZH = re.compile(r"思考|分析|规划|提纲|大纲|接下来|需要|应该|确保|例如|比如|示例|检查|要求|字数|正文|写作|开头|结尾|场景|描写")
    PRIVATE_PREFIX = "<<<AGY_FINAL"
    PRIVATE_TOKEN = re.compile(r"<<<AGY_FINAL[^<>\r\n]*>>>")
    # Only inspect an answer's opening. English itself is never evidence of
    # pollution. Hold possible self-talk until its reference to framing is clear.
    OPENING_LIMIT = 192
    SELF_TALK_STARTS = ("wait", "let me", "let's", "i need", "i must",
                        "the marker", "the boundary", "output transport contract")
    SELF_TALK = re.compile(
        r"^(?:wait\b|let me\b|let's\b|i (?:need|must)\b|the (?:marker|boundary)\b|"
        r"output transport contract\b)[\s\S]{0,160}?"
        r"(?:\b(?:exact|boundary|transport|final|private) marker\b|"
        r"\b(?:emit|output|copy|check|verify)\b[^.!?\n]{0,64}\bmarker\b|"
        r"\b(?:developer prompt|transport contract|agy_final)\b)", re.I)

    def __init__(self, marker, include_thoughts=True):
        self.marker = marker
        self.include_thoughts = include_thoughts
        self.states = {}
        self.failed = False
        self.failure_reason = ""
        self.reclassified_chars = 0
        self.last_event = None
        self.last_was_bytes = False
        self.marker_corrections = 0
        self.recovery_events = 0
        self.recovery_discarded_chars = 0
        self.repeated_markers = 0
        shape = re.fullmatch(r"<<<AGY_FINAL_([0-9a-f]{16})>>>(_*)", marker)
        self.marker_nonce = shape[1] if shape else None
        self.marker_suffix = shape[2] if shape else ""
        self.marker_pattern = (re.compile(r"<<<AGY_FINAL_([0-9a-f]{15,21})>>>" + re.escape(self.marker_suffix))
                               if shape else None)

    def _nonce_matches(self, nonce):
        expected = self.marker_nonce
        if len(nonce) == len(expected):
            return sum(a != b for a, b in zip(nonce, expected)) <= 1
        if len(nonce) == len(expected) - 1:
            return any(nonce == expected[:i] + expected[i + 1:] for i in range(len(expected)))
        extra = len(nonce) - len(expected)
        # Models sometimes duplicate a short run while copying the private nonce.
        return 1 <= extra <= 5 and any(
            nonce == expected[:i] + expected[i:i + extra] + expected[i:]
            for i in range(len(expected) - extra + 1))

    def _find_marker(self, pending):
        pos = pending.find(self.marker)
        found = (pos, len(self.marker), False) if pos >= 0 else None
        if self.marker_pattern:
            for match in self.marker_pattern.finditer(pending):
                if self._nonce_matches(match[1]):
                    if found is None or match.start() < found[0]:
                        found = (match.start(), len(match[0]), match[0] != self.marker)
                    break
        return found

    def _marker_variants(self):
        yield self.marker
        if self.marker_nonce is None:
            return
        nonce = self.marker_nonce
        for i, old in enumerate(nonce):
            for char in "0123456789abcdef":
                if char != old:
                    yield "<<<AGY_FINAL_" + nonce[:i] + char + nonce[i + 1:] + ">>>" + self.marker_suffix
        for i in range(len(nonce)):
            yield "<<<AGY_FINAL_" + nonce[:i] + nonce[i + 1:] + ">>>" + self.marker_suffix
        for size in range(1, 6):
            for i in range(len(nonce) - size + 1):
                yield "<<<AGY_FINAL_" + nonce[:i] + nonce[i:i + size] + nonce[i:] + ">>>" + self.marker_suffix

    def _partial_marker_size(self, pending, final=False):
        keep = 0
        for size in range(min(len(self.marker) - 1, len(pending)), 0, -1):
            if pending.endswith(self.marker[:size]):
                keep = size
                break
        if self.marker_nonce:
            tail = re.search(r"<<<AGY_FINAL_[0-9a-f]{0,21}>{0,2}$", pending)
            if self.marker_suffix:
                suffix = re.search(r"<<<AGY_FINAL_[0-9a-f]{15,21}>>>(_*)$", pending)
                if suffix and len(suffix[1]) < len(self.marker_suffix):
                    tail = suffix
            if tail and (not final or any(marker.startswith(tail[0]) for marker in self._marker_variants())):
                keep = max(keep, len(tail[0]))
        # Buffer malformed/private tokens too, before the ordinary 128-char
        # fallback can commit their prefix to the body.
        start = pending.rfind(self.PRIVATE_PREFIX)
        if not final and start >= 0 and ">>>" not in pending[start:]:
            keep = max(keep, min(128, len(pending) - start))
        return keep

    def _fail(self, reason):
        self.failed = True
        self.failure_reason = reason
        raise ThoughtBoundaryError(reason)

    def _validate_framing(self, pending, state, final):
        # Literal examples in already recognized planning remain in the thought
        # channel. Outside it, a private-looking token must be recoverable.
        if state["mode"] == "thought" or self.HEADER.match(pending):
            return
        for prefix in re.finditer(re.escape(self.PRIVATE_PREFIX), pending):
            end = pending.find(">>>", prefix.end())
            if re.search(r"[<\r\n]", pending[prefix.end():end if end >= 0 else None]):
                raise _FramingProblem("malformed_private_marker")
        for match in self.PRIVATE_TOKEN.finditer(pending):
            token = match[0]
            # Include a collision suffix when validating the complete marker.
            suffix = pending[match.end():match.end() + len(self.marker_suffix)]
            if state["answer_started"] and self.marker_nonce and len(token) == len(self.marker) - len(self.marker_suffix) - 1:
                raise _FramingProblem("malformed_private_marker")
            found = self._find_marker(token + suffix)
            if (not final and len(suffix) < len(self.marker_suffix)
                    and self.marker_suffix.startswith(suffix)
                    and self.marker_pattern and self._nonce_matches(token[len("<<<AGY_FINAL_"):-3])):
                continue
            if found is None or found[:2] != (0, len(token) + len(self.marker_suffix)):
                raise _FramingProblem("malformed_private_marker")
        start = pending.rfind(self.PRIVATE_PREFIX)
        if start >= 0 and ">>>" not in pending[start:]:
            tail = pending[start:]
            if len(tail) > 128 or re.search(r"[\r\n<]", tail[len(self.PRIVATE_PREFIX):]):
                raise _FramingProblem("malformed_private_marker")
            if final and self._partial_marker_size(tail, final=True) != len(tail):
                raise _FramingProblem("unfinished_private_marker")

    def _probe_answer_opening(self, state, final):
        pending = state["pending"]
        opening = pending.lstrip()
        heading = self.HEADER.match(opening)
        self_talk = self.SELF_TALK.search(opening[:self.OPENING_LIMIT + 1])
        if self_talk and self_talk.end() > self.OPENING_LIMIT:
            self_talk = None
        if self_talk and self_talk.end() == len(opening) and not final:
            # A packet end is not a word boundary: "marker" may continue as
            # "markerboard". Wait for a real delimiter or protocol completion.
            return False
        if (self_talk
                or (heading and ("\n" in heading[0] or final))):
            self._begin_recovery(state, "analysis_after_private_marker")
            return False
        lower = opening.casefold()
        possible = (not lower or self._possible_heading(opening)
                    or self.marker.startswith(opening)
                    or self.PRIVATE_PREFIX.startswith(opening)
                    or opening.startswith(self.PRIVATE_PREFIX) or any(
            phrase.startswith(lower) or lower.startswith(phrase)
            for phrase in self.SELF_TALK_STARTS))
        if possible and not final and len(pending) < self.OPENING_LIMIT:
            return False
        state["probe_opening"] = False
        return True

    def _state(self):
        return dict(pending="", mode="detect", method="", sections=set(), scan_tail="", finalized=False,
                    detect_text_false=False, heading_text_false=False, segments=deque(),
                    probe_opening=False, marker_seen=False, answer_started=False,
                    recovery_reason="", recovery_needs_answer=False)

    def _begin_recovery(self, state, reason):
        # Already emitted answer text cannot be withdrawn or safely spliced.
        if state["answer_started"]:
            self._fail(reason)
        if state["mode"] == "answer" and reason != "analysis_after_private_marker":
            prefix = state["pending"].split(self.PRIVATE_PREFIX, 1)[0]
            if prefix.strip() and not self.SELF_TALK.search(prefix[:self.OPENING_LIMIT + 1]):
                self._fail(reason)
        state.update(mode="recover", probe_opening=False, recovery_reason=reason)

    @staticmethod
    def _outside_quoted_example(text):
        fence = None
        prose = []
        for line in text.splitlines():
            match = re.match(r"^[ \t]*(`{3,}|~{3,})(.*)$", line)
            if match:
                token, tail = match.groups()
                if fence is None:
                    fence = token
                elif token[0] == fence[0] and len(token) >= len(fence) and not tail.strip():
                    fence = None
                continue
            if fence is None:
                prose.append(line)
        outside = "\n".join(prose)
        # Be conservative about multiline quotation examples, including an
        # unmatched inline code span. Ordinary apostrophes are not delimiters.
        return (fence is None
                and len(re.findall(r'(?<!\\)"', outside)) % 2 == 0
                and outside.count('`') % 2 == 0
                and outside.count('“') == outside.count('”')
                and outside.count('‘') == outside.count('’'))

    def _recover(self, state, final):
        """Wait on this SAME stream for an exact, standalone framing line.

        Never guess a boundary from language, chapter headings or an approximate
        marker inside analysis. Quoted/fenced examples are not synchronization.
        """
        pending = state["pending"]
        if (len(state["segments"]) > self.RECOVERY_PART_LIMIT
                or sum(len(json.dumps(metadata, ensure_ascii=True))
                       for _, metadata in state["segments"]) > self.RECOVERY_METADATA_LIMIT):
            self._fail("boundary_recovery_limit")
        pattern = r"(?m)^[ \t]*" + re.escape(self.marker) + r"[ \t]*\r?\n"
        for match in re.finditer(pattern, pending[:self.RECOVERY_LIMIT]):
            before = pending[:match.start()]
            # Ignore apparent delimiters inside Markdown code examples.
            if not self._outside_quoted_example(before):
                continue
            self.recovery_events += 1
            self.recovery_discarded_chars += match.end()
            parts = self._consume_parts(state, match.end())
            after = pending[match.end():]
            state.update(pending="", mode="answer", method="resynchronized_marker",
                         probe_opening=True, marker_seen=True, recovery_reason="",
                         recovery_needs_answer=True)
            return parts + self._split(after, state, final=final)
        if len(pending) > self.RECOVERY_LIMIT:
            self._fail("boundary_recovery_limit")
        if final:
            self._fail(state["recovery_reason"] or "unresolved_output_boundary")
        return []

    def _buffer_part(self, state, part):
        """Keep metadata attached to its text while the boundary is undecided."""
        metadata = {key: value for key, value in part.items() if key != "text"}
        if not part["text"] and not (metadata.keys() - {"thought"}):
            return
        segments = state["segments"]
        if segments and part["text"] and segments[-1][0] and segments[-1][1] == metadata:
            segments[-1][0] += part["text"]
        else:
            segments.append([part["text"], metadata])

    def _consume_parts(self, state, count, thought=None):
        """Emit original text spans, or discard only framing text (thought=None)."""
        parts = []
        segments = state["segments"]
        while segments and (count or not segments[0][0]):
            text, metadata = segments[0]
            if not text:
                # An empty text part can carry an independent signature. It
                # must survive even when adjacent framing text is discarded.
                if isinstance(metadata, _NativePart):
                    if self.include_thoughts or metadata.get("thought") is not True:
                        parts.append(copy.deepcopy(dict(metadata)))
                else:
                    parts.append({**copy.deepcopy(metadata), "text": ""})
                segments.popleft()
                continue
            take = min(count, len(text))
            if thought is not None:
                parts.extend(self._part(text[:take], thought, metadata))
            if take == len(text):
                segments.popleft()
            else:
                segments[0][0] = text[take:]
            count -= take
        return parts

    def _possible_heading(self, text):
        """Do not commit a split heading just because a transport sets false on every part."""
        text = text.lstrip()
        if not text or text in ("*", "**", "_", "__"):
            return True
        text = re.sub(r"^#{1,6}\s*", "", text)
        if text in ("*", "**", "_", "__"):
            return True
        text = re.sub(r"^(?:\*\*|__)", "", text)
        text = re.sub(r"\s*([:：])\s*", r"\1", text).rstrip().casefold()
        return any(("thinking process" + close + colon + end).startswith(text)
                   for close in ("", "**", "__") for colon in (":", "：")
                   for end in ("", "**", "__"))

    def _part(self, text, thought, original=None):
        if not text:
            return []
        if thought:
            self.reclassified_chars += len(text)
            if not self.include_thoughts:
                return []
        part = copy.deepcopy(original or {})
        part["text"] = text
        if thought:
            part["thought"] = True
        else:
            part.pop("thought", None)
        return [part]

    def _narrative_boundary(self, state):
        if len(state["sections"]) < 2:
            return None
        text = state["pending"]
        for match in self.PROSE.finditer(text):
            start = match.start(2)
            prefix = text[max(0, match.start() - 256):match.start()]
            sentence = match.group(2)
            # Reject quoted examples, lists, mixed-language analysis, and Chinese
            # planning vocabulary. Never switch merely upon seeing a Han glyph.
            line = prefix.rsplit("\n", 1)[-1]
            if not re.search(r"[A-Za-z]{3}", prefix):
                continue
            if line.count('"') % 2 or line.count('`') % 2 or line.count('(') > line.count(')'):
                continue
            if (len(re.findall(r"[\u4e00-\u9fff]", sentence)) < 24
                    or re.search(r"[A-Za-z*`]|[:：]", sentence)
                    or self.PLANNING_ZH.search(sentence)):
                continue
            return start
        return None

    def _split(self, text, state, original=None, final=False):
        explicit_text = (original or {}).get("thought") is False
        # A false flag already present on the leaked heading is not a new body
        # boundary when the next part repeats it. A newly introduced flag is.
        if explicit_text and state["mode"] == "thought" and not state["heading_text_false"]:
            before = state["pending"]
            state.update(pending="", mode="answer", method="native_text")
            return self._consume_parts(state, len(before), True) + self._split(text, state, original, final)
        state["pending"] += text
        if state["mode"] == "detect" and explicit_text and text:
            state["detect_text_false"] = True
        scan = state["scan_tail"] + text
        state["sections"].update(self.SECTION.findall(scan))
        state["scan_tail"] = scan[-128:]
        pending = state["pending"]
        if state["mode"] == "recover":
            return self._recover(state, final)
        match = self._find_marker(pending)
        try:
            self._validate_framing(pending[:match[0] + match[1]] if match else pending, state, final)
        except _FramingProblem as exc:
            self._begin_recovery(state, str(exc))
            return self._recover(state, final)
        if match is not None:
            pos, size, corrected = match
            before, after = pending[:pos], pending[pos + size:]
            was_answer = state["mode"] == "answer"
            if state["marker_seen"] and not state["answer_started"] and not before.strip():
                self.repeated_markers += 1
                if self.repeated_markers > self.REPEAT_LIMIT:
                    self._fail("boundary_recovery_limit")
                self.marker_corrections += int(corrected)
                parts = self._consume_parts(state, pos + size)
                state.update(pending="", probe_opening=True, recovery_needs_answer=True)
                return parts + self._split(after, state, original, final)
            if state["probe_opening"]:
                self._probe_answer_opening(state, final=True)
                if state["mode"] == "recover":
                    return self._recover(state, final)
            self.marker_corrections += int(corrected)
            state.update(pending="", mode="answer", method="corrected_marker" if corrected else "marker",
                         probe_opening=True, marker_seen=True,
                         recovery_needs_answer=state["recovery_needs_answer"] or corrected,
                         answer_started=state["answer_started"] or (was_answer and bool(before.strip())))
            parts = self._consume_parts(state, len(before), not was_answer)
            parts.extend(self._consume_parts(state, size))
            return parts + self._split(after, state, original, final)
        if state["probe_opening"] and not self._probe_answer_opening(state, final):
            return self._recover(state, final) if state["mode"] == "recover" else []
        if state["mode"] == "detect":
            heading = self.HEADER.match(pending)
            if heading and ("\n" in heading[0] or final):
                state.update(mode="thought", method="thinking_header",
                             heading_text_false=state["detect_text_false"])
            elif final or len(pending) >= self.START_LIMIT or (explicit_text and not self._possible_heading(pending)):
                state.update(mode="answer", method="native_text" if explicit_text else "plain_text")
            else:
                return []
        if state["mode"] == "thought":
            boundary = self._narrative_boundary(state)
            if boundary is not None:
                state.update(pending="", mode="answer", method="narrative_heuristic")
                return self._consume_parts(state, boundary, True) + self._split(pending[boundary:], state, original, final)
        if final and state["recovery_needs_answer"] and not state["answer_started"] and not pending.strip():
            self._fail("recovered_boundary_without_answer")
        discarded = 0
        if final:
            size = self._partial_marker_size(pending, final=True)
            if size >= len(self.PRIVATE_PREFIX):
                pending = pending[:-size]
                discarded = size
        keep = 0
        if not final:
            keep = self._partial_marker_size(pending)
            if state["mode"] == "thought":
                keep = max(keep, min(len(pending), self.LOOKBEHIND))
        safe = pending[:-keep] if keep else pending
        if state["mode"] == "answer" and safe.strip():
            state["answer_started"] = True
            state["recovery_needs_answer"] = False
        state["pending"] = pending[-keep:] if keep else ""
        parts = self._consume_parts(state, len(safe), state["mode"] == "thought")
        if discarded:
            parts.extend(self._consume_parts(state, discarded))
        if final and state["recovery_needs_answer"] and not state["answer_started"]:
            self._fail("recovered_boundary_without_answer")
        return parts

    def feed(self, chunk):
        raw = chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk
        if not isinstance(raw, str) or not raw.startswith("data:"):
            return chunk
        payload = raw[5:].strip()
        if payload == "[DONE]":
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
            state = self.states.setdefault(candidate.get("index", index), self._state())
            content = candidate.get("content") or {}
            parts = []
            for part in content.get("parts", []):
                if part.get("thought") is True:
                    # Explicit upstream thoughts and signatures are never rewritten.
                    if self.include_thoughts:
                        if state["segments"] or not part.get("text"):
                            state["segments"].append(["", _NativePart(copy.deepcopy(part))])
                            if part.get("text") and not state["pending"]:
                                # Signature-only parts must wait for real output,
                                # but must not delay following native thinking.
                                parts.extend(self._consume_parts(state, 0))
                        else:
                            parts.append(part)
                    if state["mode"] == "recover":
                        parts.extend(self._recover(state, final=False))
                elif isinstance(part.get("text"), str):
                    self._buffer_part(state, part)
                    parts.extend(self._split(part["text"], state, part))
                else:
                    # Preserve nontext parts in the same queue as buffered text.
                    # They must not overtake text awaiting boundary classification.
                    state["segments"].append(["", _NativePart(copy.deepcopy(part))])
                    parts.extend(self._consume_parts(state, 0))
                    if state["mode"] == "recover":
                        parts.extend(self._recover(state, final=False))
            if candidate.get("finishReason") not in (None, "", "FINISH_REASON_UNSPECIFIED"):
                parts.extend(self._split("", state, final=True))
                state["finalized"] = True
            candidate["content"] = {**content, "parts": parts}
        self.last_event = event
        self.last_was_bytes = isinstance(chunk, bytes)
        result = "data: " + json.dumps(event, ensure_ascii=False) + "\n\n"
        return result.encode() if self.last_was_bytes else result

    def flush(self):
        """Drain buffered text before DONE, EOF or an explicit error; no fake STOP."""
        if self.failed:
            return None
        if self.last_event is None or not any(s["pending"] or s["segments"] or s["recovery_needs_answer"] or s["mode"] == "recover" for s in self.states.values()):
            return None
        event = copy.deepcopy(self.last_event)
        response = event.get("response", event)
        response["candidates"] = []
        for index, state in self.states.items():
            parts = self._split("", state, final=True)
            state["finalized"] = True
            if parts:
                response["candidates"].append({"index": index, "content": {"role": "model", "parts": parts}})
        raw = "data: " + json.dumps(event, ensure_ascii=False) + "\n\n"
        return raw.encode() if self.last_was_bytes else raw

    @property
    def complete(self):
        return not self.failed and bool(self.states) and all(not s["pending"] and not s["segments"] and not s["recovery_needs_answer"] and s["mode"] != "recover" for s in self.states.values())

    def metadata(self):
        return {"boundary_complete": self.complete, "boundary_failed": self.failed,
                "boundary_failure_reason": self.failure_reason,
                "boundary_uncertain": any(s["mode"] in ("thought", "recover") for s in self.states.values()),
                "boundary_methods": sorted({s["method"] for s in self.states.values() if s["method"]}),
                "reclassified_chars": self.reclassified_chars,
                "boundary_marker_corrections": self.marker_corrections,
                "boundary_recovery_events": self.recovery_events,
                "boundary_recovery_discarded_chars": self.recovery_discarded_chars,
                "boundary_repeated_markers": self.repeated_markers}
