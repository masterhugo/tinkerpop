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
"""Unit tests for PR 3: AsyncGraphTraversal and AsyncGraphTraversalSource."""
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from gremlin_python.driver.async_graph_traversal import (
    AsyncGraphTraversal,
    AsyncGraphTraversalSource,
    async_traversal,
)
from gremlin_python.driver.async_resultset import AsyncResultSet, _STREAM_END
from gremlin_python.driver.remote_connection import RemoteTraversal
from gremlin_python.process.traversal import Traverser
from gremlin_python.structure.graph import Vertex


def _make_remote_traversal(items, error=None):
    q = asyncio.Queue()
    for item in items:
        q.put_nowait(item)
    q.put_nowait(_STREAM_END)
    rs = AsyncResultSet(q, "req-1")
    fut = asyncio.get_running_loop().create_future()
    if error is not None:
        fut.set_exception(error)
    else:
        fut.set_result(None)
    rs.done = fut
    return RemoteTraversal(rs)


def _make_mock_remote_conn(items=None, error=None):
    conn = MagicMock()
    conn.pdt_registry = None

    async def _submit(_gremlin_lang):
        return _make_remote_traversal(items or [], error=error)

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


class TestAsyncGraphTraversal(_AsyncioTestCase):

    async def test_async_traversal_with_and_with_remote(self):
        conn = _make_mock_remote_conn()
        g1 = async_traversal().with_(conn)
        g2 = async_traversal().with_remote(conn)
        self.assertIsInstance(g1, AsyncGraphTraversalSource)
        self.assertIsInstance(g2, AsyncGraphTraversalSource)
        self.assertIsInstance(g1.V(), AsyncGraphTraversal)

    async def test_to_list_unrolls_bulked_traversers(self):
        conn = _make_mock_remote_conn([
            Traverser("marko", 1),
            Traverser("vadas", 3),
            Traverser("josh", 2),
        ])
        g = async_traversal().with_(conn)
        result = await g.V().values("name").to_list()
        self.assertEqual(result, ["marko", "vadas", "vadas", "vadas", "josh", "josh"])
        submitted_gl = conn.submit.call_args.args[0]
        self.assertEqual(submitted_gl.get_gremlin(), "g.V().values('name')")

    async def test_to_set(self):
        conn = _make_mock_remote_conn([
            Traverser("person", 2),
            Traverser("software", 1),
        ])
        g = async_traversal().with_(conn)
        result = await g.V().label().to_set()
        self.assertEqual(result, {"person", "software"})

    async def test_next_single_and_amount_and_empty(self):
        conn = _make_mock_remote_conn([
            Traverser("a", 2),
            Traverser("b", 1),
            Traverser("c", 1),
        ])
        g = async_traversal().with_(conn)
        t = g.V().values("name")
        self.assertEqual(await t.next(), "a")
        self.assertEqual(await t.next(2), ["a", "b"])
        self.assertEqual(await t.next(), "c")
        self.assertIsNone(await t.next())

    async def test_has_next_and_next_traverser(self):
        conn = _make_mock_remote_conn([
            Traverser(Vertex(1, "person"), 2),
        ])
        g = async_traversal().with_(conn)
        t = g.V()
        self.assertTrue(await t.has_next())
        traverser = await t.next_traverser()
        self.assertIsInstance(traverser, Traverser)
        self.assertEqual(traverser.bulk, 2)
        self.assertFalse(await t.has_next())

    async def test_iterate_appends_discard_step(self):
        conn = _make_mock_remote_conn([])
        g = async_traversal().with_(conn)
        t = g.add_v("person").property("name", "hugo")
        returned = await t.iterate()
        self.assertIs(returned, t)
        submitted_gl = conn.submit.call_args.args[0]
        self.assertIn("discard()", submitted_gl.get_gremlin())

    async def test_async_for_iteration(self):
        conn = _make_mock_remote_conn([
            Traverser(10, 1),
            Traverser(20, 2),
        ])
        g = async_traversal().with_(conn)
        items = [x async for x in g.V().values("age")]
        self.assertEqual(items, [10, 20, 20])

    async def test_sync_iter_and_next_raise_helpful_error(self):
        conn = _make_mock_remote_conn([Traverser(1, 1)])
        g = async_traversal().with_(conn)
        t = g.V()
        with self.assertRaises(NotImplementedError):
            iter(t)
        with self.assertRaises(NotImplementedError):
            next(t)

    async def test_source_configuration_preserves_async_traversal_source_type(self):
        conn = _make_mock_remote_conn([Traverser(1, 1)])
        g = async_traversal().with_(conn)
        g_configured = g.with_("timeoutMillis", 5000).with_bulk(False)
        self.assertIsInstance(g_configured, AsyncGraphTraversalSource)
        res = await g_configured.V().count().next()
        self.assertEqual(res, 1)


if __name__ == "__main__":
    unittest.main()
