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
"""Unit tests for PR 4: AsyncTransaction, AsyncTransactionRemoteConnection, and execute_in_tx."""
import asyncio
import json
import struct
import unittest
from unittest.mock import AsyncMock, MagicMock

from gremlin_python.driver.aiohttp.async_transport import AsyncBufferedStream
from gremlin_python.driver.async_driver_remote_connection import (
    AsyncDriverRemoteConnection,
)
from gremlin_python.driver.async_graph_traversal import async_traversal
from gremlin_python.driver.async_resultset import AsyncResultSet, _STREAM_END
from gremlin_python.driver.async_transaction import AsyncTransaction
from gremlin_python.driver.remote_connection import RemoteTraversal
from gremlin_python.process.traversal import Traverser


def _gb_int32(value):
    return bytes([0x01, 0x00]) + struct.pack(">i", value)


def _gb_string(value):
    encoded = value.encode("utf-8")
    return bytes([0x03, 0x00]) + struct.pack(">i", len(encoded)) + encoded


def _gb_map_tx_id(tx_id):
    return (
        bytes([0x0A, 0x00])
        + struct.pack(">i", 1)
        + _gb_string("transactionId")
        + _gb_string(tx_id)
    )


def _build_gb_response(items_bytes, *, bulked=False, status_code=200):
    header = bytes([0x84, 0x01 if bulked else 0x00])
    eos = bytes([0xFD, 0x00, 0x00])
    trailer = struct.pack(">i", status_code) + bytes([0x01, 0x01])
    return header + b"".join(items_bytes) + eos + trailer


def _make_mock_transport(response_seq):
    responses = list(response_seq)
    transport = MagicMock()
    transport.connect = AsyncMock()
    transport.write = AsyncMock()
    transport.close = AsyncMock()
    transport.read_body = AsyncMock(return_value=b"")
    transport.evict_response = MagicMock()
    transport.status_code = 200
    transport.content_type = "application/vnd.graphbinary-v4.0"

    def _get_stream():
        payload = responses.pop(0) if responses else _build_gb_response([])
        chunks = iter([payload])
        mock_resp = MagicMock()
        mock_resp.content.read = AsyncMock(side_effect=lambda _n: next(chunks, b""))
        return AsyncBufferedStream(mock_resp, read_timeout=None)

    transport.get_stream = MagicMock(side_effect=_get_stream)
    return transport


def _make_remote_traversal(items):
    q = asyncio.Queue()
    for item in items:
        q.put_nowait(item)
    q.put_nowait(_STREAM_END)
    rs = AsyncResultSet(q, "req-1")
    fut = asyncio.get_running_loop().create_future()
    fut.set_result(None)
    rs.done = fut
    return RemoteTraversal(rs)


def _make_mock_remote_conn(items=None):
    conn = MagicMock()
    conn.pdt_registry = None

    async def _submit(_gremlin_lang):
        return _make_remote_traversal(items or [])

    conn.submit = AsyncMock(side_effect=_submit)
    return conn


class _AsyncioTestCase(unittest.IsolatedAsyncioTestCase):
    """IsolatedAsyncioTestCase that resets the event loop policy after each test
    so subsequent synchronous tests calling ``asyncio.get_event_loop()`` on the
    main thread do not fail with ``RuntimeError: There is no current event loop``."""

    def run(self, result=None):
        try:
            return super().run(result)
        finally:
            asyncio.set_event_loop_policy(None)


class TestAsyncTransaction(_AsyncioTestCase):

    def _make_done_rs(self, items):
        q = asyncio.Queue()
        for item in items:
            q.put_nowait(item)
        q.put_nowait(_STREAM_END)
        rs = AsyncResultSet(q)
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        rs.done = fut
        return rs

    async def test_begin_commit_lifecycle(self):
        transport = _make_mock_transport([
            _build_gb_response([_gb_map_tx_id("tx-123")]),
            _build_gb_response([_gb_int32(1)]),
            _build_gb_response([]),
        ])
        async with AsyncDriverRemoteConnection(
            "http://localhost:8182/gremlin",
            "g",
            max_connections=1,
            transport_factory=lambda: transport,
        ) as conn:
            tx = conn._client.transact()
            self.assertFalse(tx.is_open)
            gtx = await tx.begin()
            self.assertTrue(tx.is_open)
            self.assertEqual(tx.transaction_id, "tx-123")

            gtx2 = await tx.begin()
            self.assertEqual(tx.transaction_id, "tx-123")
            self.assertIs(gtx2.tx(), tx)

            res = await gtx.add_v("person").to_list()
            self.assertEqual(res, [1])

            await tx.commit()
            self.assertFalse(tx.is_open)

            with self.assertRaises(Exception):
                await tx.begin()

        self.assertEqual(transport.write.call_count, 3)
        add_v_call = transport.write.call_args_list[1].args[0]
        self.assertEqual(add_v_call["headers"]["X-Transaction-Id"], "tx-123")
        commit_call = transport.write.call_args_list[2].args[0]
        self.assertEqual(commit_call["headers"]["X-Transaction-Id"], "tx-123")
        commit_body = json.loads(commit_call["payload"].decode("utf-8"))
        self.assertEqual(commit_body["gremlin"], "g.tx().commit()")

    async def test_begin_unwraps_traverser_when_bulked(self):
        mock_client = MagicMock()
        mock_client._url = "http://localhost:8182/gremlin"
        mock_client._traversal_source = "g"
        mock_client.submit = AsyncMock(
            return_value=self._make_done_rs([Traverser({"transactionId": "tx-bulked"}, 1)])
        )
        tx = AsyncTransaction(mock_client)
        await tx.begin()
        self.assertEqual(tx.transaction_id, "tx-bulked")
        self.assertTrue(tx.is_open)

    async def test_async_with_rolls_back_uncommitted_transaction(self):
        mock_client = MagicMock()
        mock_client._url = "http://localhost:8182/gremlin"
        mock_client._traversal_source = "g"
        mock_client.submit = AsyncMock(
            side_effect=[
                self._make_done_rs([{"transactionId": "tx-rb"}]),
                self._make_done_rs([]),
            ]
        )
        async with AsyncTransaction(mock_client) as tx:
            await tx.begin()
            self.assertTrue(tx.is_open)
        self.assertFalse(tx.is_open)
        self.assertEqual(mock_client.submit.call_count, 2)
        self.assertEqual(mock_client.submit.call_args_list[1].args[0], "g.tx().rollback()")

    async def test_client_close_rolls_back_tracked_open_transaction(self):
        transport = _make_mock_transport([
            _build_gb_response([_gb_map_tx_id("tx-open")]),
            _build_gb_response([]),
        ])
        conn = AsyncDriverRemoteConnection(
            "http://localhost:8182/gremlin",
            "g",
            max_connections=1,
            transport_factory=lambda: transport,
        )
        tx = conn._client.transact()
        await tx.begin()
        self.assertTrue(tx.is_open)
        await conn.close()
        self.assertFalse(tx.is_open)
        self.assertEqual(transport.write.call_count, 2)
        rb_body = json.loads(transport.write.call_args_list[1].args[0]["payload"].decode("utf-8"))
        self.assertEqual(rb_body["gremlin"], "g.tx().rollback()")

    async def test_execute_in_tx_commits_on_success(self):
        conn = _make_mock_remote_conn()
        mock_tx = MagicMock()
        gtx = async_traversal().with_(_make_mock_remote_conn([Traverser(42, 1)]))
        mock_tx.begin = AsyncMock(return_value=gtx)
        mock_tx.commit = AsyncMock()
        mock_tx.rollback = AsyncMock()

        g = async_traversal().with_(conn)
        g.tx = MagicMock(return_value=mock_tx)

        async def _work(tx_g):
            return await tx_g.V().count().next()

        result = await g.execute_in_tx(_work)
        self.assertEqual(result, 42)
        mock_tx.begin.assert_awaited_once()
        mock_tx.commit.assert_awaited_once()
        mock_tx.rollback.assert_not_awaited()

    async def test_execute_in_tx_rolls_back_and_reraises_on_body_error(self):
        conn = _make_mock_remote_conn()
        mock_tx = MagicMock()
        gtx = async_traversal().with_(_make_mock_remote_conn([]))
        mock_tx.begin = AsyncMock(return_value=gtx)
        mock_tx.commit = AsyncMock()
        mock_tx.rollback = AsyncMock()

        g = async_traversal().with_(conn)
        g.tx = MagicMock(return_value=mock_tx)

        async def _failing_work(_tx_g):
            raise ValueError("boom in tx")

        with self.assertRaises(ValueError) as ctx:
            await g.execute_in_tx(_failing_work)
        self.assertEqual(str(ctx.exception), "boom in tx")
        mock_tx.rollback.assert_awaited_once()
        mock_tx.commit.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
