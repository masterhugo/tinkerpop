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
import asyncio
import inspect
import logging

from gremlin_python.driver import request, serializer
from gremlin_python.driver.async_connection import AsyncConnection
from gremlin_python.driver.async_resultset import AsyncResultSet
from gremlin_python.process.traversal import GremlinLang

log = logging.getLogger("gremlinpython")


class AsyncClient:
    """Native ``asyncio`` counterpart to :class:`~gremlin_python.driver.client.Client`
    for TinkerPop 4.0 HTTP streaming.

    Manages an :class:`asyncio.Queue` connection pool of :class:`AsyncConnection`
    instances without a ``ThreadPoolExecutor``, ``nest_asyncio``, or private
    per-connection event loops.
    """

    def __init__(
        self,
        url,
        traversal_source,
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
        log.info("Creating AsyncClient with url '%s'", url)

        self._closed = False
        self._url = url
        self._tracked_transactions = []
        self._enable_user_agent_on_connect = enable_user_agent_on_connect
        self._bulk_results = bulk_results
        self._traversal_source = traversal_source
        if batch_size is None:
            batch_size = 64
        self._batch_size = batch_size
        if response_serializer is None:
            response_serializer = serializer.GraphBinarySerializersV4()
        if pdt_registry is not None:
            response_serializer.configure_pdt_registry(pdt_registry)

        self._auth = auth
        self._response_serializer = response_serializer
        self._interceptors = interceptors
        self._transport_factory = transport_factory
        self._headers = headers
        self._transport_kwargs = transport_kwargs

        if pool_size is not None and max_connections == 128:
            max_connections = pool_size
        self._max_connections = max_connections
        self._pool = asyncio.Queue()
        self._fill_pool()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @property
    def available_pool_size(self) -> int:
        return self._pool.qsize()

    def response_serializer(self):
        return self._response_serializer

    @property
    def traversal_source(self):
        return self._traversal_source

    def _fill_pool(self):
        for _ in range(self._max_connections):
            conn = self._get_connection()
            self._pool.put_nowait(conn)

    def is_closed(self) -> bool:
        return self._closed

    async def close(self):
        if self._closed:
            return

        # Best-effort rollback/close of any open transactions before closing connections
        txs = list(self._tracked_transactions)
        for tx in txs:
            try:
                res = tx.close()
                if inspect.isawaitable(res):
                    await res
            except Exception:
                pass
        self._tracked_transactions.clear()

        log.info("Closing AsyncClient with url '%s'", self._url)
        closing_tasks = []
        while not self._pool.empty():
            conn = self._pool.get_nowait()
            closing_tasks.append(conn.close())
        if closing_tasks:
            await asyncio.gather(*closing_tasks)
        self._closed = True

    def track_transaction(self, tx):
        self._tracked_transactions.append(tx)

    def untrack_transaction(self, tx):
        try:
            self._tracked_transactions.remove(tx)
        except ValueError:
            pass

    def transact(self):
        """Create a new :class:`~gremlin_python.driver.async_transaction.AsyncTransaction`
        for executing operations within an explicit HTTP transaction."""
        from gremlin_python.driver.async_transaction import AsyncTransaction

        return AsyncTransaction(self)

    def _get_connection(self) -> AsyncConnection:
        return AsyncConnection(
            self._url,
            self._traversal_source,
            self._pool,
            response_serializer=self._response_serializer,
            auth=self._auth,
            interceptors=self._interceptors,
            enable_user_agent_on_connect=self._enable_user_agent_on_connect,
            bulk_results=self._bulk_results,
            transport_factory=self._transport_factory,
            headers=self._headers,
            max_connections=self._max_connections,
            **self._transport_kwargs,
        )

    async def submit(
        self, message, parameters=None, request_options=None, bindings=None
    ) -> AsyncResultSet:
        """Submit a Gremlin query string, :class:`GremlinLang`, or
        :class:`RequestMessage` asynchronously and return an :class:`AsyncResultSet`."""
        if self.is_closed():
            raise Exception("Client is closed")

        if parameters is None and bindings is not None:
            parameters = bindings

        log.debug("message '%s'", str(message))
        fields = {"g": self._traversal_source}

        if isinstance(message, GremlinLang):
            from gremlin_python.driver.driver_remote_connection import (
                DriverRemoteConnection,
            )

            extracted = DriverRemoteConnection.extract_request_options(message)
            if request_options:
                extracted.update(request_options)
            request_options = extracted
            message = message.get_gremlin()

        # Note this parameters argument only applies to string script submissions
        if isinstance(message, str) and parameters:
            if isinstance(parameters, dict):
                fields["parameters"] = GremlinLang.convert_parameters_to_string(
                    parameters
                )
            else:
                fields["parameters"] = parameters

        if isinstance(message, str):
            log.debug("fields='%s', gremlin='%s'", str(fields), str(message))
            message = request.RequestMessage(fields=fields, gremlin=message)
        else:
            # Clone the fields dict so this submit's mutations stay local
            message = message._replace(fields=dict(message.fields))

        conn = await self._pool.get()
        if request_options:
            message.fields.update(
                {
                    token: request_options[token]
                    for token in request.Tokens
                    if token in request_options and token != "parameters"
                }
            )
            if "parameters" in request_options:
                parameters_val = request_options["parameters"]
                if isinstance(parameters_val, dict):
                    parameters_val = GremlinLang.convert_parameters_to_string(
                        parameters_val
                    )
                message.fields["parameters"] = parameters_val

        if self._batch_size is not None and "batchSize" not in message.fields:
            message.fields["batchSize"] = self._batch_size

        return await conn.write(message)

    # Alias for symmetry with synchronous Client.submit_async
    submit_async = submit
