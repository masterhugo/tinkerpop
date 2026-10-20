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

import asyncio
import os
import ssl
import sys

sys.path.append("..")

from gremlin_python.driver.async_driver_remote_connection import (
    AsyncDriverRemoteConnection,
)
from gremlin_python.driver.async_graph_traversal import async_traversal
from gremlin_python.driver.auth import basic

VERTEX_LABEL = os.getenv("VERTEX_LABEL", "async_connection")


async def main():
    await with_remote()
    await with_auth()
    await with_configs()


async def with_remote():
    server_url = os.getenv("GREMLIN_SERVER_URL", "http://localhost:8182/gremlin").format(45940)
    async with AsyncDriverRemoteConnection(server_url, "g") as rc:
        g = async_traversal().with_(rc)

        await g.add_v(VERTEX_LABEL).iterate()
        count = await g.V().has_label(VERTEX_LABEL).count().next()
        print("Vertex count: " + str(count))


async def with_auth():
    server_url = os.getenv(
        "GREMLIN_SERVER_BASIC_AUTH_URL", "http://localhost:8182/gremlin"
    ).format(45941)

    if ":45941" in server_url:
        ssl_opts = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_opts.check_hostname = False
        ssl_opts.verify_mode = ssl.CERT_NONE
        rc = AsyncDriverRemoteConnection(
            server_url, "g", auth=basic("stephen", "password"), ssl=ssl_opts
        )
    else:
        rc = AsyncDriverRemoteConnection(
            server_url, "g", auth=basic("stephen", "password")
        )

    async with rc:
        g = async_traversal().with_(rc)
        await g.add_v(VERTEX_LABEL).iterate()
        count = await g.V().has_label(VERTEX_LABEL).count().next()
        print("Vertex count: " + str(count))


async def with_configs():
    server_url = os.getenv("GREMLIN_SERVER_URL", "http://localhost:8182/gremlin").format(45940)

    async def add_headers(request):
        request.headers["x-custom-header"] = "async-example"

    async with AsyncDriverRemoteConnection(
        server_url,
        "g",
        max_connections=8,
        connect_timeout_millis=5000,
        idle_timeout_millis=180000,
        keep_alive_time_millis=30000,
        batch_size=64,
        compression="none",
        interceptors=[add_headers],
    ) as rc:
        g = async_traversal().with_(rc)
        await g.add_v(VERTEX_LABEL).iterate()
        count = await g.V().has_label(VERTEX_LABEL).count().next()
        print("Vertex count: " + str(count))


if __name__ == "__main__":
    asyncio.run(main())
