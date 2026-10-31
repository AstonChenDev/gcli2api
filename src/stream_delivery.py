"""Per-response ASGI delivery evidence; independent of upstream completion."""
import json
import time
from contextvars import ContextVar

from anyio import CancelScope
from log import log
from src.stream_metadata import StreamCompletion
from src.usage_log import new_request_id

current_delivery = ContextVar('current_stream_delivery', default=None)


class StreamDelivery:
    def __init__(self):
        self.request_id = new_request_id()
        self.disconnect_source = None
        self.exception_type = None
        self.body_complete = False
        self.bytes_sent = 0
        self.last_send_at = None
        self.error_sent = False
        self.terminal_sent = False
        self.openai_candidates = set()
        self.openai_finished = set()
        self.gemini_completion = StreamCompletion()
        self.callbacks = []
        self.closers = []
        self.closed = False

    @property
    def complete(self):
        return not self.error_sent and (self.body_complete or self.terminal_sent)

    def sent(self, message):
        body = message.get('body', b'')
        self.bytes_sent += len(body)
        self.last_send_at = time.time()
        # Application converters emit whole SSE records, not arbitrary sockets.
        for line in body.decode('utf-8', errors='replace').splitlines():
            if not line.startswith('data:'):
                continue
            payload = line[5:].strip()
            if payload == '[DONE]':
                self.terminal_sent = True
                continue
            try:
                event = json.loads(payload)
            except ValueError:
                continue
            if not isinstance(event, dict):
                continue
            event = event.get('response', event)
            if not isinstance(event, dict):
                continue
            self.error_sent |= bool(event.get('error')) or event.get('type') == 'error'
            if 'candidates' in event:
                self.gemini_completion.observe('data: ' + json.dumps(event))
                self.terminal_sent = self.gemini_completion.complete
            if event.get('type') == 'message_stop':
                self.terminal_sent = True
            choices = event.get('choices')
            for position, choice in enumerate(choices if isinstance(choices, list) else []):
                if not isinstance(choice, dict):
                    continue
                index = choice.get('index', position)
                if not isinstance(index, int) or not 0 <= index < 128:
                    continue
                self.openai_candidates.add(index)
                if choice.get('finish_reason'):
                    self.openai_finished.add(index)
                self.terminal_sent = (len(self.openai_candidates) >= self.gemini_completion.expected_candidates
                    and self.openai_candidates.issubset(self.openai_finished))
        if not message.get('more_body', False):
            self.body_complete = True

    def metadata(self):
        return dict(downstream_complete=self.complete,
                    downstream_body_complete=self.body_complete,
                    downstream_bytes_sent=self.bytes_sent,
                    downstream_last_send_at=self.last_send_at,
                    disconnect_source=self.disconnect_source,
                    downstream_exception_type=self.exception_type)

    async def begin_segment(self):
        # A continuation starts only after the preceding segment was consumed.
        # Settle that completed API call before the next call can fail; leaving
        # it until the outer response closes would reorder usage-log results.
        with CancelScope(shield=True):
            for callback in self.callbacks:
                await callback(continuing=True)
            self.callbacks.clear()
        self.terminal_sent = False
        self.openai_candidates.clear()
        self.openai_finished.clear()
        self.gemini_completion = StreamCompletion()

    async def finalize(self):
        if self.closed:
            return
        self.closed = True
        with CancelScope(shield=True):
            for close in self.closers:
                try:
                    await close()
                except Exception as exc:
                    log.warning(f'Stream cleanup failed: {type(exc).__name__}')
            for callback in self.callbacks:
                await callback()
            log.info('[STREAM_DELIVERY] ' + json.dumps(
                dict(request_id=self.request_id, **self.metadata())))
