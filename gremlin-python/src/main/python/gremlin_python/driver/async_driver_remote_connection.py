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
import logging

from gremlin_python.driver import serializer
from gremlin_python.driver.async_client import AsyncClient
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.remote_connection import RemoteConnection, RemoteTraversal

log = logging.getLogger("gremlinpython")


class AsyncDriverRemoteConnection(RemoteConnection):
    """Native ``asyncio`` counterpart to
    :class:`~gremlin_python.driver.driver_remote_connection.DriverRemoteConnection`
    for TinkerPop 4.0 HTTP streaming.

    Wraps an :class:`~gremlin_python.driver.async_client.AsyncClient` and
    submits :class:`~gremlin_python.process.traversal.GremlinLang` traversals
    over HTTP without threads or ``nest_asyncio``.
    """

    def __init__(
        self,
        url,
        traversal_source="g",
        max_connections=128,
        response_serializer=None,
        interceptors=None,
        auth=None,
        enable_user_agent_on_connect=True,
        bulk_results=False,
        pdt_registry=None,
        batch_size=None,
        transport_factory=None,
        headers=None,
        pool_size=None,
        **transport_kwargs,
    ):
        log.info("Creating AsyncDriverRemoteConnection with url '%s'", str(url))
        self.__url = url
        self.__traversal_source = traversal_source
        self.__max_connections = max_connections
        self.__auth = auth
        self.__enable_user_agent_on_connect = enable_user_agent_on_connect
        self.__bulk_results = bulk_results
        self.__transport_kwargs = transport_kwargs
        self.pdt_registry = pdt_registry

        if response_serializer is None:
            response_serializer = serializer.GraphBinarySerializersV4()

        self._client = AsyncClient(
            url,
            traversal_source,
            max_connections=max_connections,
            response_serializer=response_serializer,
            interceptors=interceptors,
            auth=auth,
            enable_user_agent_on_connect=enable_user_agent_on_connect,
            bulk_results=bulk_results,
            pdt_registry=pdt_registry,
            batch_size=batch_size,
            transport_factory=transport_factory,
            headers=headers,
            pool_size=pool_size,
            **transport_kwargs,
        )
        self._url = self._client._url
        self._traversal_source = self._client._traversal_source

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        log.info("Closing AsyncDriverRemoteConnection with url '%s'", str(self._url))
        await self._client.close()

    def is_closed(self) -> bool:
        return self._client.is_closed()

    async def submit(self, gremlin_lang) -> RemoteTraversal:
        log.debug("submit with gremlin lang script '%s'", gremlin_lang.get_gremlin())
        result_set = await self._client.submit(
            gremlin_lang.get_gremlin(),
            request_options=self.extract_request_options(gremlin_lang),
        )
        return RemoteTraversal(result_set)

    submit_async = submit

    @staticmethod
    def extract_request_options(gremlin_lang):
        return DriverRemoteConnection.extract_request_options(gremlin_lang)
