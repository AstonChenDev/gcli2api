from typing import Any, AsyncIterator

from fastapi import Request, Response
from fastapi.responses import StreamingResponse
from anyio import CancelScope, create_task_group
from src.stream_delivery import StreamDelivery, current_delivery


class ObservedStreamingResponse(StreamingResponse):
    def __init__(self, content, delivery, **kwargs):
        super().__init__(content, **kwargs)
        self.delivery = delivery
        self.headers['X-Request-ID'] = delivery.request_id

    async def __call__(self, scope, receive, send):
        state = self.delivery
        token = current_delivery.set(state)

        async def observed_receive():
            message = await receive()
            if message['type'] == 'http.disconnect':
                state.disconnect_source = 'http.disconnect'
            return message

        async def observed_send(message):
            try:
                await send(message)
            except OSError as exc:
                state.disconnect_source = 'send_failure'
                state.exception_type = type(exc).__name__
                raise
            if message['type'] == 'http.response.body':
                state.sent(message)

        try:
            version = tuple(map(int, scope.get('asgi', {}).get('spec_version', '2.0').split('.')))
            if version >= (2, 4):
                # ASGI 2.4 StreamingResponse relies on a failed send. There may
                # be no next send while the model is thinking, so still watch
                # disconnects. Older ASGI already has its own receive watcher.
                async with create_task_group() as tasks:
                    async def watch_disconnect():
                        await self.listen_for_disconnect(observed_receive)
                        tasks.cancel_scope.cancel()

                    tasks.start_soon(watch_disconnect)
                    try:
                        await super().__call__(scope, observed_receive, observed_send)
                    finally:
                        tasks.cancel_scope.cancel()
            else:
                await super().__call__(scope, observed_receive, observed_send)
        except BaseException as exc:
            state.exception_type = state.exception_type or type(exc).__name__
            raise
        finally:
            try:
                with CancelScope(shield=True):
                    try:
                        await self.body_iterator.aclose()
                    finally:
                        await state.finalize()
            finally:
                current_delivery.reset(token)


async def prepend_async_item(first_item: Any, iterator: AsyncIterator[Any]):
    """Yield a prefetched item before continuing the original iterator."""
    yield first_item
    async for item in iterator:
        yield item


async def read_first_async_item(iterator: AsyncIterator[Any]) -> Any:
    """Python 3.9-compatible async equivalent of built-in anext()."""
    return await iterator.__anext__()


async def build_streaming_response_or_error(
    iterator: AsyncIterator[Any],
    media_type: str = "text/event-stream",
    request: Request = None,
):
    """
    Prefetch the first async item so router code can return an upstream error
    response directly before FastAPI commits a 200 streaming response.
    """
    state = StreamDelivery()
    token = current_delivery.set(state)
    # Close the outer converter too, including when it yielded a Response or
    # was interrupted before ObservedStreamingResponse could take ownership.
    close = getattr(iterator, "aclose", None)
    if close:
        state.closers.append(close)
    try:
        try:
            if request is None:
                first_item = await read_first_async_item(iterator)
            else:
                # Request bodies have already been parsed by FastAPI. Only this
                # watcher owns receive until prefetch completes; it is joined
                # before StreamingResponse starts its own disconnect listener.
                exhausted = False
                async with create_task_group() as tasks:
                    async def watch_disconnect():
                        while True:
                            if (await request.receive())["type"] == "http.disconnect":
                                state.disconnect_source = "http.disconnect"
                                tasks.cancel_scope.cancel()
                                return

                    tasks.start_soon(watch_disconnect)
                    try:
                        first_item = await read_first_async_item(iterator)
                    except StopAsyncIteration:
                        exhausted = True
                    finally:
                        tasks.cancel_scope.cancel()
                if state.disconnect_source:
                    await state.finalize()
                    return Response(status_code=499, headers={'X-Request-ID': state.request_id})
                if exhausted:
                    raise StopAsyncIteration
        except StopAsyncIteration:
            await state.finalize()
            return Response(status_code=204, headers={'X-Request-ID': state.request_id})
    except BaseException:
        await state.finalize()
        raise
    finally:
        current_delivery.reset(token)

    if isinstance(first_item, Response):
        await state.finalize()
        first_item.headers['X-Request-ID'] = state.request_id
        return first_item

    return ObservedStreamingResponse(
        prepend_async_item(first_item, iterator),
        state,
        media_type=media_type,
    )
