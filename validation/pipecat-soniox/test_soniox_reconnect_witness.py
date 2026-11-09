"""Bounded offline regression to run against an unmodified Pipecat checkout."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pipecat.frames.frames import BotStoppedSpeakingFrame, InterruptionFrame, TTSStartedFrame
from pipecat.services import websocket_service
from pipecat.services.soniox import stt as soniox_stt
from tests.test_soniox_stt import _agent_text_service, _drain, _FakeWebsocket, _pushed, _word


@pytest.mark.parametrize(
    ("reconnect", "active_interruptible"),
    [(True, False), (True, True), (False, False)],
    ids=["active-uninterruptible-reconnect", "active-interruptible-reconnect", "same-socket"],
)
@pytest.mark.asyncio
async def test_reconnect_retires_in_flight_agent_text(monkeypatch, reconnect, active_interruptible):
    service, observer, original = _agent_text_service()
    replacement = _FakeWebsocket([])
    replacement.ping = AsyncMock()
    clock = SimpleNamespace(get_time=lambda: 0)
    old_wait_started = asyncio.Event()
    release_old_wait = asyncio.Event()
    valid_new_word_sent = asyncio.Event()
    target = replacement if reconnect else original

    async def capture_new_word(raw):
        message = json.loads(raw)
        if message.get("type") == "agent_text" and message["text"].strip() == "new due":
            valid_new_word_sent.set()

    target.send.side_effect = capture_new_word

    async def old_sleep(delay):
        assert delay > 0
        old_wait_started.set()
        await release_old_wait.wait()

    async def fake_connect(*args, **kwargs):
        return replacement

    monkeypatch.setattr(service, "get_clock", lambda: clock)
    monkeypatch.setattr(soniox_stt, "asyncio", SimpleNamespace(
        sleep=old_sleep, Event=asyncio.Event, wait_for=asyncio.wait_for,
    ))
    monkeypatch.setattr(websocket_service, "websocket_connect", fake_connect)

    def agent_text(socket):
        return [
            value for call in socket.send.await_args_list
            if (value := json.loads(call.args[0])).get("type") == "agent_text"
        ]

    async def wait_for_dequeue():
        while not service._agent_text_words.empty():
            await asyncio.sleep(0)

    try:
        await observer.on_push_frame(_pushed(_word(
            "old active", pts=10_000_000_000, interruptible=active_interruptible,
        )))
        if active_interruptible:
            await asyncio.wait_for(wait_for_dequeue(), timeout=1)
        else:
            await asyncio.wait_for(old_wait_started.wait(), timeout=1)
        assert service._agent_text_words.empty()

        if reconnect:
            await observer.on_push_frame(
                _pushed(_word("old queued", pts=11_000_000_000, interruptible=False))
            )
            assert service._agent_text_words.qsize() == 1
            assert await asyncio.wait_for(service._reconnect_websocket(1), timeout=1)
            assert original.closed
            replacement.ping.assert_awaited_once()
        else:
            await observer.on_push_frame(_pushed(InterruptionFrame()))

        clock.get_time = lambda: 20_000_000_000
        await observer.on_push_frame(_pushed(TTSStartedFrame()))
        await observer.on_push_frame(_pushed(_word("new due", pts=12_000_000_000)))
        try:
            await asyncio.wait_for(valid_new_word_sent.wait(), timeout=1)
            new_word_before_old_release = True
        except TimeoutError:
            new_word_before_old_release = False

        release_old_wait.set()
        await _drain(service)
        await observer.on_push_frame(_pushed(BotStoppedSpeakingFrame()))
        end = {"type": "agent_text", "text": "", "end_of_segment": True}

        print("\nSONIOX_RECONNECT_WITNESS " + json.dumps({
            "reconnect": reconnect,
            "active_interruptible": active_interruptible,
            "new_word_before_old_release": new_word_before_old_release,
            "original": agent_text(original),
            "replacement": agent_text(replacement),
        }, sort_keys=True))

        if reconnect:
            assert agent_text(original) == []
            assert (new_word_before_old_release, agent_text(replacement)) == (
                True, [{"type": "agent_text", "text": "new due", "end_of_segment": False}, end],
            ), "soniox_connection_generation_contract"
        else:
            assert not new_word_before_old_release
            assert agent_text(original) == [
                {"type": "agent_text", "text": "old active", "end_of_segment": False},
                {"type": "agent_text", "text": " new due", "end_of_segment": False},
                end,
            ]
            assert agent_text(replacement) == []
    finally:
        release_old_wait.set()
        for task in service.task_manager.current_tasks():
            await service.task_manager.cancel_task(task, timeout=1)
