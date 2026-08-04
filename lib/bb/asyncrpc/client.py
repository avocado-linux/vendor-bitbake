#
# Copyright BitBake Contributors
#
# SPDX-License-Identifier: GPL-2.0-only
#

import abc
import asyncio
import json
import os
import socket
import sys
import re
import time
import contextlib
from threading import Thread
from .connection import StreamConnection, WebsocketConnection, DEFAULT_MAX_CHUNK
from .exceptions import ConnectionClosedError, InvokeError

UNIX_PREFIX = "unix://"
WS_PREFIX = "ws://"
WSS_PREFIX = "wss://"

ADDR_TYPE_UNIX = 0
ADDR_TYPE_TCP = 1
ADDR_TYPE_WS = 2

# Retries after a failed request, once a connection is established.
MAX_REQUEST_RETRIES = 3
# Retries after a failed connect. Upstream's `count >= 3` allowed 4 attempts and
# this keeps that, because the wall clock is bounded by RETRY_DEADLINE below
# rather than by the attempt count - capping attempts was the wrong lever, since
# it also halved tolerance for the cheap ECONNREFUSED case.
MAX_CONNECT_RETRIES = 3
# Sleep between connect retries. Without one, every attempt is spent inside the
# few hundred ms a restarting hashserv/prserv spends rebinding, because
# ECONNREFUSED returns in microseconds - so the retries all land in the same
# refusal window and buy nothing.
CONNECT_RETRY_BACKOFF = 0.2
# Ceiling on the wall clock one call may spend across ALL its retries, as a
# multiple of self.timeout. This, not the attempt counts, is what bounds a wedged
# peer: splitting the budgets did not help the common wedge shape, because the
# kernel completes the TCP handshake for a process that is listening but blocked,
# so the client gets a socket and the failure bills the REQUEST budget - four
# attempts of self.timeout each, 120s at the default.
RETRY_DEADLINE_TIMEOUTS = 2

def parse_address(addr):
    if addr.startswith(UNIX_PREFIX):
        return (ADDR_TYPE_UNIX, (addr[len(UNIX_PREFIX) :],))
    elif addr.startswith(WS_PREFIX) or addr.startswith(WSS_PREFIX):
        return (ADDR_TYPE_WS, (addr,))
    else:
        m = re.match(r"\[(?P<host>[^\]]*)\]:(?P<port>\d+)$", addr)
        if m is not None:
            host = m.group("host")
            port = m.group("port")
        else:
            host, port = addr.split(":")

        return (ADDR_TYPE_TCP, (host, int(port)))

class AsyncClient(object):
    def __init__(
        self,
        proto_name,
        proto_version,
        logger,
        timeout=30,
        server_headers=False,
        headers={},
    ):
        self.socket = None
        self.max_chunk = DEFAULT_MAX_CHUNK
        self.proto_name = proto_name
        self.proto_version = proto_version
        self.logger = logger
        self.timeout = timeout
        self.needs_server_headers = server_headers
        self.server_headers = {}
        self.headers = headers

    async def connect_tcp(self, address, port):
        async def connect_sock():
            # Bound the connect on self.timeout. asyncio.open_connection never
            # times out on its own, so a server whose accept queue is momentarily
            # not serviced (e.g. under concurrent multi-node load) would wedge the
            # client forever - only reads were bounded before. A negative timeout
            # is StreamConnection.recv()'s "unbounded" sentinel, so pass None
            # rather than letting wait_for expire instantly on it.
            timeout = self.timeout if self.timeout >= 0 else None
            # No shield here. A cancelled open_connection() does not orphan its
            # socket: BaseEventLoop._connect_sock and
            # _create_connection_transport both close what they made from a bare
            # `except:`, which catches CancelledError, and streams.open_connection
            # has no await between create_connection returning and returning the
            # (reader, writer) pair - so there is no point at which cancellation
            # can be delivered to a connection nobody will close. Verified on
            # 3.14 plus 300 cancelled connects: fd count flat, no
            # ResourceWarnings.
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(address, port), timeout
                )
            except asyncio.TimeoutError:
                # Mirror StreamConnection.recv(): asyncio.TimeoutError is not an
                # OSError before Python 3.11, so _send_wrapper's retry catch would
                # miss it and a slow accept queue would fail hard instead of
                # taking the reconnect path this timeout exists to feed.
                raise ConnectionError(
                    "Timed out connecting to %s:%s" % (address, port)
                )
            return StreamConnection(reader, writer, self.timeout, self.max_chunk)

        self._connect_sock = connect_sock

    async def connect_unix(self, path):
        async def connect_sock():
            # AF_UNIX has path length issues so chdir here to workaround
            cwd = os.getcwd()
            try:
                os.chdir(os.path.dirname(path))
                # The socket must be opened synchronously so that CWD doesn't get
                # changed out from underneath us so we pass as a sock into asyncio
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
                # Bound the blocking connect the same way connect_tcp/websocket
                # bound theirs; asyncio takes the socket non-blocking below. A
                # negative timeout is StreamConnection.recv()'s "unbounded"
                # sentinel, and settimeout() rejects it with a ValueError that
                # _send_wrapper does not catch - so leave the socket unbounded
                # instead, matching what the sentinel asks for.
                if self.timeout >= 0:
                    sock.settimeout(self.timeout)
                try:
                    sock.connect(os.path.basename(path))
                except Exception:
                    # Close the fd if the bounded connect times out or fails;
                    # otherwise the socket leaks on every failed retry.
                    sock.close()
                    raise
                sock.settimeout(None)
            finally:
                os.chdir(cwd)
            reader, writer = await asyncio.open_unix_connection(sock=sock)
            return StreamConnection(reader, writer, self.timeout, self.max_chunk)

        self._connect_sock = connect_sock

    async def connect_websocket(self, uri):
        import websockets

        async def connect_sock():
            # A negative timeout is StreamConnection.recv()'s "unbounded"
            # sentinel; websockets spells that None. Passing the negative value
            # straight through inverts the sentinel into "time out at once".
            open_timeout = self.timeout if self.timeout >= 0 else None
            try:
                websocket = await websockets.connect(
                    uri,
                    ping_interval=None,
                    open_timeout=open_timeout,
                )
            except asyncio.TimeoutError:
                # Same conversion connect_tcp does, and for the same reason:
                # before Python 3.11 asyncio.TimeoutError is not an OSError, so
                # _send_wrapper's retry catch would miss the open_timeout that
                # websockets raises here and a stalled accept queue would fail
                # hard instead of taking the reconnect path.
                raise ConnectionError("Timed out connecting to %s" % uri)
            return WebsocketConnection(websocket, self.timeout)

        self._connect_sock = connect_sock

    async def setup_connection(self):
        # Send headers
        await self.socket.send("%s %s" % (self.proto_name, self.proto_version))
        await self.socket.send(
            "needs-headers: %s" % ("true" if self.needs_server_headers else "false")
        )
        for k, v in self.headers.items():
            await self.socket.send("%s: %s" % (k, v))

        # End of headers
        await self.socket.send("")

        self.server_headers = {}
        if self.needs_server_headers:
            while True:
                line = await self.socket.recv()
                if not line:
                    # End headers
                    break
                tag, value = line.split(":", 1)
                self.server_headers[tag.lower()] = value.strip()

    async def get_header(self, tag, default):
        await self.connect()
        return self.server_headers.get(tag, default)

    async def connect(self):
        if self.socket is None:
            self.socket = await self._connect_sock()
            await self.setup_connection()

    async def disconnect(self):
        if self.socket is not None:
            await self.socket.close()
            self.socket = None

    async def close(self):
        await self.disconnect()

    async def _send_wrapper(self, proc):
        # Connect failures and request failures are retried on separate budgets,
        # because they fail on different timescales: a request retry normally
        # costs a round trip, while a connect against a blackholed peer costs a
        # full self.timeout because the SYN is neither answered nor refused.
        #
        # The attempt counts alone do NOT bound a wedged peer, though. The more
        # likely wedge is a process that is alive and listening but blocked (NFS
        # I/O, an sqlite lock, GIL starvation): the kernel completes the
        # handshake on its behalf, so the client gets a socket, self.socket is
        # non-None, and every subsequent timeout bills the REQUEST budget at
        # self.timeout apiece. The deadline below is what actually caps that, and
        # it applies to whichever budget the failure lands on.
        deadline = None
        if self.timeout and self.timeout > 0:
            deadline = time.monotonic() + self.timeout * RETRY_DEADLINE_TIMEOUTS
        connect_count = 0
        count = 0
        while True:
            try:
                await self.connect()
                return await proc()
            except (
                OSError,
                ConnectionError,
                ConnectionClosedError,
                json.JSONDecodeError,
                UnicodeDecodeError,
            ) as e:
                self.logger.warning("Error talking to server: %s" % e)
                # self.socket is set the instant _connect_sock() hands one back,
                # and cleared by close() below, so it is exactly the "did we get
                # a socket at all" discriminator - no separate flag to keep in
                # step with connect()'s two phases. Anything that fails with a
                # socket already in hand cost a round trip rather than a connect
                # timeout: the header exchange in setup_connection(), hashserv's
                # auth() on top of it, or the request itself. Those bill the
                # request budget.
                connecting = self.socket is None
                if connecting:
                    spent, limit = connect_count, MAX_CONNECT_RETRIES
                else:
                    spent, limit = count, MAX_REQUEST_RETRIES
                out_of_time = deadline is not None and time.monotonic() >= deadline
                if spent >= limit or out_of_time:
                    if not isinstance(e, ConnectionError):
                        raise ConnectionError(str(e))
                    raise e
                await self.close()
                if connecting:
                    connect_count += 1
                    # Space the connect retries out. See CONNECT_RETRY_BACKOFF:
                    # against ECONNREFUSED all the attempts otherwise complete
                    # inside the same rebind window and none of them outlast it.
                    if CONNECT_RETRY_BACKOFF > 0:
                        await asyncio.sleep(CONNECT_RETRY_BACKOFF)
                else:
                    count += 1

    def check_invoke_error(self, msg):
        if isinstance(msg, dict) and "invoke-error" in msg:
            raise InvokeError(msg["invoke-error"]["message"])

    async def invoke(self, msg):
        async def proc():
            await self.socket.send_message(msg)
            return await self.socket.recv_message()

        result = await self._send_wrapper(proc)
        self.check_invoke_error(result)
        return result

    async def ping(self):
        return await self.invoke({"ping": {}})

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


class Client(object):
    def __init__(self):
        self.client = self._get_async_client()
        self.loop = asyncio.new_event_loop()

        # Override any pre-existing loop.
        # Without this, the PR server export selftest triggers a hang
        # when running with Python 3.7.  The drawback is that there is
        # potential for issues if the PR and hash equiv (or some new)
        # clients need to both be instantiated in the same process.
        # This should be revisited if/when Python 3.9 becomes the
        # minimum required version for BitBake, as it seems not
        # required (but harmless) with it.
        asyncio.set_event_loop(self.loop)

        self._add_methods("connect_tcp", "ping")

    @abc.abstractmethod
    def _get_async_client(self):
        pass

    def _get_downcall_wrapper(self, downcall):
        def wrapper(*args, **kwargs):
            return self.loop.run_until_complete(downcall(*args, **kwargs))

        return wrapper

    def _add_methods(self, *methods):
        for m in methods:
            downcall = getattr(self.client, m)
            setattr(self, m, self._get_downcall_wrapper(downcall))

    def connect_unix(self, path):
        self.loop.run_until_complete(self.client.connect_unix(path))
        self.loop.run_until_complete(self.client.connect())

    @property
    def max_chunk(self):
        return self.client.max_chunk

    @max_chunk.setter
    def max_chunk(self, value):
        self.client.max_chunk = value

    def disconnect(self):
        self.loop.run_until_complete(self.client.close())

    def close(self):
        if self.loop:
            self.loop.run_until_complete(self.client.close())
            if sys.version_info >= (3, 6):
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()
        self.loop = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False


class ClientPool(object):
    def __init__(self, max_clients):
        self.avail_clients = []
        self.num_clients = 0
        self.max_clients = max_clients
        self.loop = None
        self.client_condition = None

    @abc.abstractmethod
    async def _new_client(self):
        raise NotImplementedError("Must be implemented in derived class")

    def close(self):
        if self.client_condition:
            self.client_condition = None

        if self.loop:
            self.loop.run_until_complete(self.__close_clients())
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()
            self.loop = None

    def run_tasks(self, tasks):
        if not self.loop:
            self.loop = asyncio.new_event_loop()

        thread = Thread(target=self.__thread_main, args=(tasks,))
        thread.start()
        thread.join()

    @contextlib.asynccontextmanager
    async def get_client(self):
        async with self.client_condition:
            if self.avail_clients:
                client = self.avail_clients.pop()
            elif self.num_clients < self.max_clients:
                self.num_clients += 1
                client = await self._new_client()
            else:
                while not self.avail_clients:
                    await self.client_condition.wait()
                client = self.avail_clients.pop()

        try:
            yield client
        finally:
            async with self.client_condition:
                self.avail_clients.append(client)
                self.client_condition.notify()

    def __thread_main(self, tasks):
        async def process_task(task):
            async with self.get_client() as client:
                await task(client)

        asyncio.set_event_loop(self.loop)
        if not self.client_condition:
            self.client_condition = asyncio.Condition()
        tasks = [process_task(t) for t in tasks]
        self.loop.run_until_complete(asyncio.gather(*tasks))

    async def __close_clients(self):
        for c in self.avail_clients:
            await c.close()
        self.avail_clients = []
        self.num_clients = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False
