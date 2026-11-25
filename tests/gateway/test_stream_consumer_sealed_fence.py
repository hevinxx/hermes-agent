"""A streamed overflow seal that lands inside a ``` fence keeps the code block in both messages.

The sealed head is closed by ``_send_or_edit``, but the continuation started as bare code lines:
the next message rendered the code as prose and the block's closing ``` then opened a new block
over the prose after it.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.platforms.base import BasePlatformAdapter
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig
from tests.waiting import async_wait_until


def _adapter():
    cls = type("ChatAdapter", (BasePlatformAdapter,), {"MAX_MESSAGE_LENGTH": 1000})
    cls.__abstractmethods__ = frozenset()
    adapter = cls.__new__(cls)
    adapter._typing_paused = set()
    adapter._fatal_error_message = None
    ids = iter(f"m{i}" for i in range(100))
    adapter.send = AsyncMock(side_effect=lambda **kw: SimpleNamespace(success=True, message_id=next(ids)))
    adapter.edit_message = AsyncMock(
        side_effect=lambda **kw: SimpleNamespace(success=True, message_id=kw["message_id"]))
    return adapter


def _final_per_message(adapter) -> list[str]:
    """The last text each message id ended with, in send order."""
    final: dict[str, str] = {}
    for call in adapter.send.call_args_list:
        final.setdefault(f"m{len(final)}", call.kwargs["content"])
    for call in adapter.edit_message.call_args_list:
        final[call.kwargs["message_id"]] = call.kwargs["content"]
    return list(final.values())


async def _stream(adapter, *deltas: str) -> None:
    consumer = GatewayStreamConsumer(adapter, "chat", StreamConsumerConfig(
        edit_interval=0.01, buffer_threshold=5, cursor=""))
    task = asyncio.create_task(consumer.run())
    for delta in deltas:
        before = len(adapter.send.call_args_list) + len(adapter.edit_message.call_args_list)
        consumer.on_delta(delta)
        await async_wait_until(
            lambda: consumer._queue.empty()
            and len(adapter.send.call_args_list) + len(adapter.edit_message.call_args_list) > before,
            timeout=5, what="delta delivered")
    consumer.finish()
    await task


@pytest.mark.asyncio
async def test_seal_inside_a_fence_closes_the_head_and_reopens_the_continuation():
    adapter = _adapter()
    code = "".join(f"value_{i:03d} = compute({i})\n" for i in range(60))
    # An open preview first, so the overflow goes through the seal-the-existing-message path.
    await _stream(adapter, "Here is the script:\n", "```python\n" + code + "```\nDone.")

    messages = _final_per_message(adapter)
    assert len(messages) >= 2  # really sealed + continued
    for text in messages:
        assert text.count("```") % 2 == 0, text
    head, continuation = messages[0], messages[1]
    assert head.rstrip().endswith("```")
    assert continuation.startswith("```python\n")
    assert continuation.rstrip().endswith("Done.")
