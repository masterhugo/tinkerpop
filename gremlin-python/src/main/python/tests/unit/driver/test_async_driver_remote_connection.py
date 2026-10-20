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
"""Unit tests for PR 3: AsyncDriverRemoteConnection."""
import asyncio
import struct
import unittest
from unittest.mock import AsyncMock, MagicMock

from gremlin_python.driver.aiohttp.async_transport import AsyncBufferedStream
from gremlin_python.driver.async_driver_remote_connection import (
    AsyncDriverRemoteConnection,
)
from gremlin_python.process.strategies import OptionsStrategy
from gremlin_python.process.traversal import GremlinLang


class _AsyncioTestCase(unittest.IsolatedAsyncioTestCase):
    """IsolatedAsyncioTestCase that resets the event loop policy after each test
    so subsequent synchronous tests calling ``asyncio.get_event_loop()`` on the
    main thread do not fail with ``RuntimeError: There is no current event loop``."""

    def run(self, result=None):
        try:
            return super().run(result)
        finally:
            asyncio.set_event_loop_policy(None)


def _gb_int32(value):
    return bytes([0x01, 0x00]) + struct.pack(">i", value)


def _build_gb_response(items_bytes, *, bulked=False, status_code=200):
    header = bytes([0x84, 0x01 if bulked else 0x00])
    eos = bytes([0xFD, 0x00, 0x00])
    trailer = struct.pack(">i", status_code) + bytes([0x01, 0x01])
    return header + b"".join(items_bytes) + eos + trailer


def _make_mock_transport(response_seq):
    """Create a mock transport where each write() serves the next response in response_seq."""
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


class TestAsyncDriverRemoteConnection(_AsyncioTestCase):

    async def test_submit_gremlin_lang_returns_remote_traversal(self):
        transport = _make_mock_transport([_build_gb_response([_gb_int32(10), _gb_int32(20)])])
        async with AsyncDriverRemoteConnection(
            "http://localhost:8182/gremlin",
            "g",
            max_connections=1,
            transport_factory=lambda: transport,
        ) as conn:
            gl = GremlinLang()
            gl.add_step("V")
            gl.add_step("count")
            remote_traversal = await conn.submit(gl)
            items = await remote_traversal.traversers.all()
            self.assertEqual(items, [10, 20])
            self.assertFalse(conn.is_closed())

        self.assertTrue(conn.is_closed())
        transport.close.assert_awaited_once()

    async def test_extract_request_options_includes_bulk_results_and_options_strategy(self):
        gl = GremlinLang()
        gl.add_source("withStrategies", OptionsStrategy(timeoutMillis=2500))
        gl.add_step("V")
        opts = AsyncDriverRemoteConnection.extract_request_options(gl)
        self.assertEqual(opts["timeoutMillis"], 2500)
        self.assertTrue(opts["bulkResults"])


if __name__ == "__main__":
    unittest.main()
