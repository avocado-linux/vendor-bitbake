#
# BitBake Tests for the asyncrpc client
#
# Copyright BitBake Contributors
#
# SPDX-License-Identifier: GPL-2.0-only
#

import asyncio
import logging
import sys
import time
import types
import unittest
from unittest import mock

from bb.asyncrpc.client import (
    CONNECT_RETRY_BACKOFF,
    MAX_CONNECT_RETRIES,
    MAX_REQUEST_RETRIES,
    RETRY_DEADLINE_TIMEOUTS,
    AsyncClient,
)
from bb.asyncrpc.exceptions import ConnectionClosedError

logger = logging.getLogger("bitbake.tests.asyncrpc")


class FakeSocket(object):
    async def close(self):
        pass


class CountingClient(AsyncClient):
    """An AsyncClient whose connect, handshake and request phases fail on demand.

    Each phase counts its own attempts, so a test can assert which retry budget
    _send_wrapper charged a failure to rather than only that it gave up.
    """

    def __init__(self, sock_fails=False, handshake_fails=False, proc_fails=False):
        super().__init__("test", "1.0", logger, timeout=1)
        self.sock_fails = sock_fails
        self.handshake_fails = handshake_fails
        self.proc_fails = proc_fails
        self.sock_attempts = 0
        self.handshake_attempts = 0
        self.proc_attempts = 0

        async def connect_sock():
            self.sock_attempts += 1
            if self.sock_fails:
                raise ConnectionError("connection refused")
            return FakeSocket()

        self._connect_sock = connect_sock

    async def setup_connection(self):
        self.handshake_attempts += 1
        if self.handshake_fails:
            raise ConnectionClosedError("server closed during header exchange")

    async def run(self):
        async def proc():
            self.proc_attempts += 1
            if self.proc_fails:
                raise ConnectionClosedError("server closed mid-request")
            return "ok"

        return await self._send_wrapper(proc)


class RetryBudgets(unittest.TestCase):
    def test_socket_failure_uses_the_connect_budget(self):
        client = CountingClient(sock_fails=True)

        with self.assertRaises(ConnectionError):
            asyncio.run(client.run())

        # Never got a socket, so every attempt costs a full connect timeout
        # against a wedged peer - the reason this budget is the smaller one.
        # Spelled out rather than derived from the constant: `MAX_CONNECT_RETRIES
        # + 1` restates the implementation, so it would keep passing if the budget
        # were changed by accident. Upstream allowed 4 connect attempts and this
        # keeps that, so 4 is the number under test.
        self.assertEqual(MAX_CONNECT_RETRIES, 3)
        self.assertEqual(client.sock_attempts, 4)
        self.assertEqual(client.handshake_attempts, 0)

    def test_handshake_failure_uses_the_request_budget(self):
        client = CountingClient(handshake_fails=True)

        with self.assertRaises(ConnectionError):
            asyncio.run(client.run())

        # The socket was established every time and the header exchange failed
        # over it. That is a round trip, not a connect, so it bills the request
        # budget - an overloaded server that accepts then drops during headers
        # must not burn the connect budget's single retry.
        self.assertEqual(MAX_REQUEST_RETRIES, 3)
        self.assertEqual(client.handshake_attempts, 4)
        self.assertEqual(client.sock_attempts, 4)

    def test_request_failure_uses_the_request_budget(self):
        client = CountingClient(proc_fails=True)

        with self.assertRaises(ConnectionError):
            asyncio.run(client.run())

        self.assertEqual(client.proc_attempts, 4)

    def test_a_successful_call_does_not_retry(self):
        client = CountingClient()

        self.assertEqual(asyncio.run(client.run()), "ok")

        self.assertEqual(client.sock_attempts, 1)
        self.assertEqual(client.handshake_attempts, 1)
        self.assertEqual(client.proc_attempts, 1)


class WebsocketConnect(unittest.TestCase):
    def test_open_timeout_is_converted_to_connection_error(self):
        async def fake_connect(uri, **kwargs):
            # What websockets raises when open_timeout expires. Before Python
            # 3.11 this is asyncio.TimeoutError, which is not an OSError, so it
            # escapes _send_wrapper's retry catch entirely.
            raise asyncio.TimeoutError("timed out during opening handshake")

        client = AsyncClient("test", "1.0", logger, timeout=1)
        fake = types.ModuleType("websockets")
        fake.connect = fake_connect

        with mock.patch.dict(sys.modules, {"websockets": fake}):
            asyncio.run(client.connect_websocket("ws://example.invalid:1234"))
            with self.assertRaises(ConnectionError):
                asyncio.run(client._connect_sock())

    def test_negative_timeout_leaves_the_open_unbounded(self):
        seen = {}

        async def fake_connect(uri, **kwargs):
            seen.update(kwargs)
            return object()

        client = AsyncClient("test", "1.0", logger, timeout=-1)
        fake = types.ModuleType("websockets")
        fake.connect = fake_connect

        with mock.patch.dict(sys.modules, {"websockets": fake}):
            asyncio.run(client.connect_websocket("ws://example.invalid:1234"))
            asyncio.run(client._connect_sock())

        # A negative timeout is the "unbounded" sentinel the connection classes
        # honour; websockets spells that None. Passing the negative value
        # through would invert it into "time out immediately".
        self.assertIsNone(seen["open_timeout"])


class SlowClient(AsyncClient):
    """A client whose socket is obtained but whose request always times out.

    This is the wedge shape that matters in practice: the peer is alive and
    listening but blocked, so the kernel completes the handshake for it. The
    client therefore HAS a socket, and every failure bills the request budget.
    """

    def __init__(self, per_attempt, timeout=1):
        super().__init__("test", "1.0", logger, timeout=timeout)
        self.per_attempt = per_attempt
        self.proc_attempts = 0

        async def connect_sock():
            return FakeSocket()

        self._connect_sock = connect_sock

    async def setup_connection(self):
        pass

    async def run(self):
        async def proc():
            self.proc_attempts += 1
            # Stand in for StreamConnection.recv() hitting self.timeout.
            await asyncio.sleep(self.per_attempt)
            raise ConnectionClosedError("timed out waiting for the server")

        return await self._send_wrapper(proc)


class WedgedPeerIsBounded(unittest.TestCase):
    def test_a_blocked_peer_is_capped_by_the_deadline_not_the_attempt_count(self):
        # timeout=1 and RETRY_DEADLINE_TIMEOUTS=2 gives a 2s ceiling. Each attempt
        # burns a full timeout, so the attempt count alone would allow 4s.
        client = SlowClient(per_attempt=1, timeout=1)

        started = time.monotonic()
        with self.assertRaises(ConnectionError):
            asyncio.run(client.run())
        elapsed = time.monotonic() - started

        self.assertEqual(RETRY_DEADLINE_TIMEOUTS, 2)
        # Stopped on the deadline, so fewer attempts than the request budget
        # permits. Without the deadline this is 4 attempts and about 4s - the 120s
        # wedge at the default timeout of 30.
        self.assertLess(client.proc_attempts, 4)
        self.assertLessEqual(elapsed, 3.0)

    def test_the_deadline_does_not_cut_a_call_that_is_making_progress(self):
        # Fast failures must still get the full request budget; the deadline is a
        # wall-clock ceiling, not a retry reduction.
        client = SlowClient(per_attempt=0, timeout=1)

        with self.assertRaises(ConnectionError):
            asyncio.run(client.run())

        self.assertEqual(client.proc_attempts, 4)


class ConnectTimeout(unittest.TestCase):
    def test_a_connect_that_never_completes_raises_ConnectionError(self):
        # The wedge fix itself: asyncio.open_connection never times out on its
        # own, so without the wait_for a blackholed peer hangs the client
        # forever. ConnectionError (not TimeoutError) is required, because
        # _send_wrapper's retry catch is what feeds the reconnect path and
        # asyncio.TimeoutError is not an OSError before 3.11.
        client = AsyncClient("test", "1.0", logger, timeout=0.25)

        async def never_connects(*args, **kwargs):
            await asyncio.sleep(3600)

        async def go():
            await client.connect_tcp("192.0.2.1", 1234)
            with mock.patch("asyncio.open_connection", never_connects):
                # Bounded well above the client's own 0.25s timeout, purely so a
                # regression that drops that timeout FAILS here instead of
                # hanging. An unbounded test for "must not hang" hangs when it
                # regresses, and a hung job reads as flaky infrastructure and gets
                # retried rather than fixed. asyncio.TimeoutError is not a
                # ConnectionError, so the assertRaises below does not absorb it.
                await asyncio.wait_for(client._connect_sock(), 5)

        started = time.monotonic()
        with self.assertRaises(ConnectionError):
            asyncio.run(go())
        elapsed = time.monotonic() - started

        # Bounded by the client's timeout rather than by the sleep above.
        self.assertLess(elapsed, 2.0)

    def test_a_negative_timeout_means_unbounded_rather_than_instant(self):
        client = AsyncClient("test", "1.0", logger, timeout=-1)
        seen = {}

        async def record(*args, **kwargs):
            seen["called"] = True
            reader = mock.Mock()
            writer = mock.Mock()
            return reader, writer

        async def go():
            await client.connect_tcp("192.0.2.1", 1234)
            with mock.patch("asyncio.open_connection", record):
                return await client._connect_sock()

        asyncio.run(go())
        # It got through instead of expiring immediately on the sentinel.
        self.assertTrue(seen["called"])


class ConnectBackoff(unittest.TestCase):
    def test_connect_retries_sleep_between_attempts(self):
        # Against ECONNREFUSED every attempt returns in microseconds, so without a
        # sleep all four land inside the few hundred ms a restarting server spends
        # rebinding and the retries buy nothing.
        self.assertGreater(CONNECT_RETRY_BACKOFF, 0)

        client = CountingClient(sock_fails=True)
        slept = []

        real_sleep = asyncio.sleep

        async def record_sleep(delay, *args, **kwargs):
            slept.append(delay)
            return await real_sleep(0)

        async def go():
            with mock.patch("asyncio.sleep", record_sleep):
                await client.run()

        with self.assertRaises(ConnectionError):
            asyncio.run(go())

        # One sleep per retry, not per attempt: 4 attempts, 3 gaps.
        self.assertEqual(slept, [CONNECT_RETRY_BACKOFF] * 3)
