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
import types
import unittest
from unittest import mock

from bb.asyncrpc.client import (
    MAX_CONNECT_RETRIES,
    MAX_REQUEST_RETRIES,
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
        self.assertEqual(client.sock_attempts, MAX_CONNECT_RETRIES + 1)
        self.assertEqual(client.handshake_attempts, 0)

    def test_handshake_failure_uses_the_request_budget(self):
        client = CountingClient(handshake_fails=True)

        with self.assertRaises(ConnectionError):
            asyncio.run(client.run())

        # The socket was established every time and the header exchange failed
        # over it. That is a round trip, not a connect, so it bills the request
        # budget - an overloaded server that accepts then drops during headers
        # must not burn the connect budget's single retry.
        self.assertEqual(client.handshake_attempts, MAX_REQUEST_RETRIES + 1)
        self.assertEqual(client.sock_attempts, MAX_REQUEST_RETRIES + 1)

    def test_request_failure_uses_the_request_budget(self):
        client = CountingClient(proc_fails=True)

        with self.assertRaises(ConnectionError):
            asyncio.run(client.run())

        self.assertEqual(client.proc_attempts, MAX_REQUEST_RETRIES + 1)

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
