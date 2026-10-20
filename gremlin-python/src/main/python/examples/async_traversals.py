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
import sys

sys.path.append("..")

from gremlin_python.driver.async_driver_remote_connection import (
    AsyncDriverRemoteConnection,
)
from gremlin_python.driver.async_graph_traversal import async_traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P, T

VERTEX_LABEL = os.getenv("VERTEX_LABEL", "async_person")


async def main():
    server_url = os.getenv("GREMLIN_SERVER_URL", "http://localhost:8182/gremlin").format(45940)

    async with AsyncDriverRemoteConnection(server_url, "g") as rc:
        g = async_traversal().with_(rc)

        # Adding vertices concurrently or sequentially using native await
        v1 = await g.add_v(VERTEX_LABEL).property("name", "marko").next()
        v2 = await g.add_v(VERTEX_LABEL).property("name", "stephen").next()
        v3 = await g.add_v(VERTEX_LABEL).property("name", "vadas").next()

        await g.V(v1).add_e("knows").to(v2).property("weight", 0.75).iterate()
        await g.V(v1).add_e("knows").to(v3).property("weight", 0.75).iterate()

        marko = await g.V().has(VERTEX_LABEL, "name", "marko").values("name").next()
        print("name: " + str(marko))

        people_marko_knows = (
            await g.V()
            .has(VERTEX_LABEL, "name", "marko")
            .out("knows")
            .values("name")
            .to_list()
        )
        for person in people_marko_knows:
            print("marko knows " + str(person))

        # Async iteration over streamed results
        async for vertex_id in g.V().has_label(VERTEX_LABEL).id_():
            print("streamed vertex id: " + str(vertex_id))


if __name__ == "__main__":
    asyncio.run(main())
