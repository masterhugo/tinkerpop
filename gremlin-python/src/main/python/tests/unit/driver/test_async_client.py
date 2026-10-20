#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""Unit tests for PR 2: AsyncConnection and AsyncClient."""
import asyncio
import json
import struct
import unittest
from unittest.mock import AsyncMock, MagicMock

from aiohttp.client_exceptions import ClientOSError

from gremlin_python.driver import useragent
from gremlin_python.driver.aiohttp.async_transport import AsyncBufferedStream
from gremlin_python.driver.async_client import AsyncClient
from gremlin_python.driver.async_connection import AsyncConnection
from gremlin_python.driver.connection import GremlinConnectionError, GremlinServerError
from gremlin_python.driver.request import RequestMessage
from gremlin_python.process.traversal import GremlinLang


def _gb_int32(value):
    return bytes([0x01, 0x00]) + struct.pack(">i", value)


def _gb_string(value):
    encoded = value.encode("utf-8")
    return bytes([0x03, 0x00]) + struct.pack(">i", len(encoded)) + encoded


def _build_gb_response(items_bytes, *, bulked=False, status_code=200):
    header = bytes([0x84, 0x01 if bulked else 0x00])
    eos = bytes([0xFD, 0x00, 0x00])
    trailer = struct.pack(">i", status_code) + bytes([0x01, 0x01])
    return header + b"".join(items_bytes) + eos + trailer


def _make_mock_transport(response_bytes=None, *, status_code=200,
                         content_type="application/vnd.graphbinary-v4.0",
                         body_bytes=b""):
    if response_bytes is None:
        response_bytes = _build_gb_response([_gb_int32(42)])

    transport = MagicMock()
    transport.connect = AsyncMock()
    transport.write = AsyncMock()
    transport.close = AsyncMock()
    transport.read_body = AsyncMock(return_value=body_bytes)
    transport.evict_response = MagicMock()
    transport.status_code = status_code
    transport.content_type = content_type

    def _get_stream():
        chunks = iter([response_bytes])
        mock_resp = MagicMock()
        mock_resp.content.read = AsyncMock(side_effect=lambda _n: next(chunks, b""))
        return AsyncBufferedStream(mock_resp, read_timeout=None)

    transport.get_stream = MagicMock(side_effect=_get_stream)
    return transport


class _AsyncioTestCase(unittest.IsolatedAsyncioTestCase):
    """IsolatedAsyncioTestCase that resets the event loop policy after each test
    so subsequent synchronous tests calling ``asyncio.get_event_loop()`` on the
    main thread do not fail with ``RuntimeError: There is no current event loop``."""

    def run(self, result=None):
        try:
            return super().run(result)
        finally:
            asyncio.set_event_loop_policy(None)


class TestAsyncConnection(_AsyncioTestCase):

    async def test_connect_passes_user_agent_and_bulk_results_headers(self):
        pool = asyncio.Queue()
        transport = _make_mock_transport()
        conn = AsyncConnection(
            "http://localhost:8182/gremlin",
            "g",
            pool,
            enable_user_agent_on_connect=True,
            bulk_results=True,
            transport_factory=lambda: transport,
        )
        await conn.connect()
        transport.connect.assert_awaited_once_with(
            "http://localhost:8182/gremlin",
            {
                useragent.userAgentHeader: useragent.userAgent,
                "bulkResults": "true",
            },
        )
        await conn.close()
        transport.close.assert_awaited_once()

    async def test_interceptor_validation(self):
        pool = asyncio.Queue()
        fn = lambda req: None
        conn1 = AsyncConnection("http://localhost:8182/gremlin", "g", pool, interceptors=fn)
        self.assertEqual(conn1._interceptors, [fn])

        conn2 = AsyncConnection("http://localhost:8182/gremlin", "g", pool, interceptors=(fn,))
        self.assertEqual(conn2._interceptors, [fn])

        auth_fn = lambda req: None
        conn3 = AsyncConnection(
            "http://localhost:8182/gremlin", "g", pool, interceptors=[fn], auth=auth_fn
        )
        self.assertEqual(conn3._interceptors, [fn, auth_fn])

        with self.assertRaises(TypeError):
            AsyncConnection("http://localhost:8182/gremlin", "g", pool, interceptors=12345)

    async def test_write_executes_sync_and_async_interceptors_and_promotes_tx_id(self):
        pool = asyncio.Queue()
        transport = _make_mock_transport(_build_gb_response([_gb_string("ok")]))
        order = []

        def sync_interceptor(req):
            order.append("sync")
            req.headers["X-Sync"] = "1"

        async def async_interceptor(req):
            await asyncio.sleep(0)
            order.append("async")
            req.headers["X-Async"] = "2"

        conn = AsyncConnection(
            "http://localhost:8182/gremlin",
            "g",
            pool,
            interceptors=[sync_interceptor, async_interceptor],
            transport_factory=lambda: transport,
        )

        msg = RequestMessage(
            fields={"g": "g", "transactionId": "tx-999"},
            gremlin="g.V()",
        )
        rs = await conn.write(msg)
        self.assertEqual(await rs.all(), ["ok"])
        self.assertEqual(order, ["sync", "async"])

        sent_msg = transport.write.call_args.args[0]
        self.assertEqual(sent_msg["headers"]["X-Transaction-Id"], "tx-999")
        self.assertEqual(sent_msg["headers"]["X-Sync"], "1")
        self.assertEqual(sent_msg["headers"]["X-Async"], "2")
        self.assertEqual(pool.qsize(), 1)

    async def test_write_failure_returns_connection_to_pool_and_wraps_transport_error(self):
        pool = asyncio.Queue()
        transport = _make_mock_transport()
        transport.write = AsyncMock(side_effect=ClientOSError("broken pipe"))
        conn = AsyncConnection(
            "http://localhost:8182/gremlin",
            "g",
            pool,
            transport_factory=lambda: transport,
        )
        msg = RequestMessage(fields={"g": "g"}, gremlin="g.V()")
        with self.assertRaises(GremlinConnectionError):
            await conn.write(msg)
        self.assertEqual(pool.qsize(), 1)

    async def test_receive_204_no_content_returns_empty_and_recycles_connection(self):
        pool = asyncio.Queue()
        transport = _make_mock_transport(b"", status_code=204)
        conn = AsyncConnection(
            "http://localhost:8182/gremlin",
            "g",
            pool,
            transport_factory=lambda: transport,
        )
        msg = RequestMessage(fields={"g": "g"}, gremlin="g.V().drop()")
        rs = await conn.write(msg)
        self.assertEqual(await rs.all(), [])
        self.assertEqual(pool.qsize(), 1)

    async def test_receive_non_graphbinary_error_raises_gremlin_server_error(self):
        pool = asyncio.Queue()
        transport = _make_mock_transport(
            status_code=500,
            content_type="text/plain",
            body_bytes=b"Internal Server Error",
        )
        conn = AsyncConnection(
            "http://localhost:8182/gremlin",
            "g",
            pool,
            transport_factory=lambda: transport,
        )
        msg = RequestMessage(fields={"g": "g"}, gremlin="g.V()")
        rs = await conn.write(msg)
        with self.assertRaises(GremlinServerError) as ctx:
            await rs.all()
        self.assertEqual(ctx.exception.status_code, 500)
        self.assertIn("Internal Server Error", str(ctx.exception))
        transport.evict_response.assert_called_once()
        self.assertEqual(pool.qsize(), 1)

    async def test_receive_empty_body_raises_connection_error(self):
        pool = asyncio.Queue()
        transport = _make_mock_transport(b"", status_code=200)
        conn = AsyncConnection(
            "http://localhost:8182/gremlin",
            "g",
            pool,
            transport_factory=lambda: transport,
        )
        msg = RequestMessage(fields={"g": "g"}, gremlin="g.V()")
        rs = await conn.write(msg)
        with self.assertRaises(GremlinConnectionError) as ctx:
            await rs.all()
        self.assertIn("empty response body", str(ctx.exception))
        transport.evict_response.assert_called_once()
        self.assertEqual(pool.qsize(), 1)


class TestAsyncClient(_AsyncioTestCase):

    async def test_pool_initialization_and_close(self):
        transports = []

        def factory():
            t = _make_mock_transport()
            transports.append(t)
            return t

        async with AsyncClient(
            "http://localhost:8182/gremlin",
            "g",
            max_connections=4,
            transport_factory=factory,
        ) as client:
            self.assertEqual(client.available_pool_size, 4)
            self.assertFalse(client.is_closed())
            rs = await client.submit("g.V()")
            self.assertEqual(await rs.all(), [42])
            self.assertEqual(client.available_pool_size, 4)

        self.assertTrue(client.is_closed())
        self.assertEqual(len(transports), 1)
        transports[0].close.assert_awaited_once()

        with self.assertRaises(Exception):
            await client.submit("g.V()")

    async def test_submit_string_with_parameters_and_request_options(self):
        transport = _make_mock_transport(_build_gb_response([_gb_int32(7)]))
        client = AsyncClient(
            "http://localhost:8182/gremlin",
            "g",
            max_connections=1,
            batch_size=32,
            transport_factory=lambda: transport,
        )
        rs = await client.submit(
            "g.V(x)",
            parameters={"x": 1},
            request_options={" evaluationTimeout": 5000, "transactionId": "tx-1"},
        )
        self.assertEqual(await rs.all(), [7])
        sent = transport.write.call_args.args[0]
        self.assertEqual(sent["headers"]["X-Transaction-Id"], "tx-1")
        body = json.loads(sent["payload"].decode("utf-8"))
        self.assertEqual(body["gremlin"], "g.V(x)")
        self.assertEqual(body["g"], "g")
        self.assertEqual(body["batchSize"], 32)
        self.assertEqual(body["transactionId"], "tx-1")
        self.assertIn("parameters", body)
        await client.close()

    async def test_submit_gremlin_lang(self):
        transport = _make_mock_transport(_build_gb_response([_gb_int32(99)]))
        client = AsyncClient(
            "http://localhost:8182/gremlin",
            "g",
            max_connections=1,
            transport_factory=lambda: transport,
        )
        gl = GremlinLang()
        gl.add_step("V")
        gl.add_step("limit", 5)
        rs = await client.submit(gl)
        self.assertEqual(await rs.all(), [99])
        sent = transport.write.call_args.args[0]
        body = json.loads(sent["payload"].decode("utf-8"))
        self.assertEqual(body["gremlin"], "g.V().limit(5)")
        self.assertTrue(body["bulkResults"])
        await client.close()

    async def test_submit_request_message_does_not_mutate_caller_fields(self):
        transport = _make_mock_transport(_build_gb_response([_gb_int32(1)]))
        client = AsyncClient(
            "http://localhost:8182/gremlin",
            "g",
            max_connections=1,
            batch_size=64,
            transport_factory=lambda: transport,
        )
        original_fields = {"g": "g"}
        msg = RequestMessage(fields=original_fields, gremlin="g.V()")
        rs = await client.submit(msg, request_options={"transactionId": "tx-abc"})
        await rs.all()
        self.assertEqual(original_fields, {"g": "g"})
        await client.close()

    async def test_concurrent_submissions_share_pool(self):
        def factory():
            return _make_mock_transport(_build_gb_response([_gb_int32(10), _gb_int32(20)]))

        async with AsyncClient(
            "http://localhost:8182/gremlin",
            "g",
            max_connections=2,
            transport_factory=factory,
        ) as client:
            async def _query():
                rs = await client.submit("g.V()")
                return await rs.all()

            results = await asyncio.gather(*[_query() for _ in range(6)])
            self.assertEqual(results, [[10, 20]] * 6)
            self.assertEqual(client.available_pool_size, 2)


if __name__ == "__main__":
    unittest.main()
