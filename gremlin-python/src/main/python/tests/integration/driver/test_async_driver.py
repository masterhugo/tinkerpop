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
"""Integration tests for AsyncClient, AsyncDriverRemoteConnection,
AsyncGraphTraversal, and AsyncTransaction over TinkerPop 4.0 HTTP streaming."""
import asyncio
import os
import ssl
import unittest

from gremlin_python.driver.async_client import AsyncClient
from gremlin_python.driver.async_driver_remote_connection import (
    AsyncDriverRemoteConnection,
)
from gremlin_python.driver.async_graph_traversal import async_traversal
from gremlin_python.driver.auth import basic
from gremlin_python.driver.connection import GremlinServerError
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P
from gremlin_python.structure.graph import Vertex

gremlin_server_url = os.environ.get("GREMLIN_SERVER_URL", "http://localhost:{}/gremlin")
gremlin_basic_auth_url = os.environ.get(
    "GREMLIN_SERVER_BASIC_AUTH_URL", "https://localhost:{}/gremlin"
)
anonymous_url = gremlin_server_url.format(45940)
basic_url = gremlin_basic_auth_url.format(45941)


class _AsyncioTestCase(unittest.IsolatedAsyncioTestCase):
    """IsolatedAsyncioTestCase that resets the event loop policy after each test
    so subsequent synchronous tests calling ``asyncio.get_event_loop()`` on the
    main thread do not fail with ``RuntimeError: There is no current event loop``."""

    def run(self, result=None):
        try:
            return super().run(result)
        finally:
            asyncio.set_event_loop_policy(None)


class TestAsyncClientIntegration(_AsyncioTestCase):

    async def test_submit_and_all(self):
        async with AsyncClient(anonymous_url, "gmodern") as client:
            rs = await client.submit("g.V().count()")
            res = await rs.all()
            self.assertEqual(res, [6])

    async def test_submit_async_for_streaming(self):
        async with AsyncClient(anonymous_url, "gmodern") as client:
            rs = await client.submit("g.V().values('name')")
            names = [name async for name in rs]
            self.assertEqual(sorted(names), ["josh", "lop", "marko", "peter", "ripple", "vadas"])

    async def test_concurrent_gather_submissions(self):
        async with AsyncClient(anonymous_url, "gmodern", max_connections=4) as client:
            async def _fetch_count():
                rs = await client.submit("g.V().count()")
                return await rs.all()

            results = await asyncio.gather(*[_fetch_count() for _ in range(8)])
            self.assertEqual(results, [[6]] * 8)

    async def test_basic_auth_over_tls(self):
        ssl_opts = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_opts.check_hostname = False
        ssl_opts.verify_mode = ssl.CERT_NONE
        async with AsyncClient(
            basic_url,
            "gmodern",
            auth=basic("stephen", "password"),
            ssl=ssl_opts,
        ) as client:
            rs = await client.submit("g.V().count()")
            self.assertEqual(await rs.all(), [6])

    async def test_invalid_query_raises_gremlin_server_error(self):
        async with AsyncClient(anonymous_url, "gmodern") as client:
            rs = await client.submit("g.V().nonExistentStep()")
            with self.assertRaises(GremlinServerError):
                await rs.all()


class TestAsyncTraversalIntegration(_AsyncioTestCase):

    async def test_traversal_terminal_steps(self):
        async with AsyncDriverRemoteConnection(anonymous_url, "gmodern") as conn:
            g = async_traversal().with_(conn)
            self.assertEqual(await g.V().count().next(), 6)
            self.assertEqual(await g.V(1).next(), Vertex(1))

            names = await g.V().values("name").to_list()
            self.assertEqual(len(names), 6)

            labels = await g.V().label().to_set()
            self.assertEqual(labels, {"person", "software"})

            two = await g.V().values("name").next(2)
            self.assertEqual(len(two), 2)

            filtered = await (
                g.with_side_effect("a", ["josh", "peter"])
                .V(1)
                .out("created")
                .in_("created")
                .values("name")
                .where(P.within("a"))
                .to_list()
            )
            self.assertEqual(sorted(filtered), ["josh", "peter"])


class TestAsyncTransactionIntegration(_AsyncioTestCase):

    async def asyncSetUp(self):
        async with AsyncClient(anonymous_url, "gtx") as client:
            rs = await client.submit("g.V().drop()")
            await rs.all()

    async def test_commit_and_rollback_transaction(self):
        async with AsyncDriverRemoteConnection(anonymous_url, "gtx") as conn:
            g = async_traversal().with_(conn)

            # Commit
            tx = g.tx()
            gtx = await tx.begin()
            await gtx.add_v("person").property("name", "alice").iterate()
            self.assertEqual(await g.V().has_label("person").count().next(), 0)
            await tx.commit()
            self.assertEqual(await g.V().has_label("person").count().next(), 1)

            # Rollback
            tx2 = g.tx()
            gtx2 = await tx2.begin()
            await gtx2.add_v("person").property("name", "bob").iterate()
            await tx2.rollback()
            self.assertEqual(await g.V().has_label("person").count().next(), 1)

    async def test_execute_in_tx(self):
        async with AsyncDriverRemoteConnection(anonymous_url, "gtx") as conn:
            g = async_traversal().with_(conn)

            async def _create_person(gtx):
                await gtx.add_v("person").property("name", "charlie").iterate()
                return await gtx.V().has("person", "name", "charlie").count().next()

            count = await g.execute_in_tx(_create_person)
            self.assertEqual(count, 1)
            self.assertEqual(await g.V().has("person", "name", "charlie").count().next(), 1)


if __name__ == "__main__":
    unittest.main()
