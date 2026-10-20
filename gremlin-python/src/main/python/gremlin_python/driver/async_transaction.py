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

from gremlin_python.driver.remote_connection import RemoteConnection, RemoteTraversal
from gremlin_python.process.traversal import Traverser
from gremlin_python.structure.graph import Graph

log = logging.getLogger("gremlinpython")


class AsyncTransaction:
    """Native ``asyncio`` counterpart to :class:`~gremlin_python.driver.transaction.Transaction`
    for TinkerPop 4.0 HTTP transactions.

    Controls an explicit remote transaction over HTTP (``g.tx().begin()``,
    ``g.tx().commit()``, ``g.tx().rollback()``) and attaches ``transactionId``
    to every request submitted within the transaction.
    """

    def __init__(self, client):
        self._client = client
        self._transaction_id = None
        self._is_open = False
        self._failed = False

    async def begin(self):
        """Start the HTTP transaction and return a transaction-bound
        :class:`~gremlin_python.driver.async_graph_traversal.AsyncGraphTraversalSource`.

        Idempotent while open; raises if called after the transaction has been
        closed or failed.
        """
        if self._failed:
            raise Exception(
                "Transaction is closed and cannot be reused; begin a new transaction"
            )

        if not self._is_open:
            try:
                result = await self._client.submit("g.tx().begin()")
                results = await result.all()
            except Exception:
                self._failed = True
                raise

            if not results:
                self._failed = True
                raise Exception("Server did not return transaction ID")

            result_map = results[0]
            if isinstance(result_map, Traverser):
                result_map = result_map.object

            if isinstance(result_map, dict):
                self._transaction_id = result_map.get("transactionId")
            else:
                self._failed = True
                raise Exception(
                    "Server did not return transaction ID in expected format"
                )

            if not self._transaction_id:
                self._failed = True
                raise Exception("Server returned empty transaction ID")

            self._is_open = True
            self._client.track_transaction(self)

        from gremlin_python.driver.async_graph_traversal import (
            AsyncGraphTraversalSource,
            AsyncRemoteStrategy,
            AsyncTraversalStrategies,
        )
        from gremlin_python.process.traversal import GremlinLang

        tx_connection = AsyncTransactionRemoteConnection(self)
        strategies = AsyncTraversalStrategies()
        strategies.add_strategies([AsyncRemoteStrategy(tx_connection)])
        return AsyncGraphTraversalSource(Graph(), strategies, GremlinLang())

    async def submit(self, gremlin, parameters=None, request_options=None):
        """Submit a ``gremlin-lang`` string within this transaction with
        ``transactionId`` automatically attached."""
        if not self._is_open:
            raise Exception("Transaction is not open")
        opts = {"transactionId": self._transaction_id}
        if request_options:
            opts.update(request_options)
        return await self._client.submit(
            gremlin, parameters=parameters, request_options=opts
        )

    async def commit(self):
        """Commit the transaction."""
        await self._close_transaction("g.tx().commit()")

    async def rollback(self):
        """Roll back the transaction."""
        await self._close_transaction("g.tx().rollback()")

    async def _close_transaction(self, script):
        if not self._is_open:
            raise Exception("Transaction is not open")
        rs = await self._client.submit(
            script, request_options={"transactionId": self._transaction_id}
        )
        await rs.all()
        self._is_open = False
        self._failed = True
        self._client.untrack_transaction(self)

    @property
    def is_open(self) -> bool:
        return self._is_open

    @property
    def transaction_id(self):
        return self._transaction_id

    async def close(self):
        if self._is_open:
            await self.rollback()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._is_open:
            await self.rollback()
        return False


class AsyncTransactionRemoteConnection(RemoteConnection):
    """An async ``RemoteConnection`` that attaches ``transactionId`` to all
    requests submitted through a transaction-bound
    :class:`~gremlin_python.driver.async_graph_traversal.AsyncGraphTraversalSource`."""

    def __init__(self, transaction: AsyncTransaction):
        super().__init__(
            transaction._client._url,
            transaction._client._traversal_source,
        )
        self._transaction = transaction

    async def submit(self, gremlin_lang) -> RemoteTraversal:
        if not self._transaction.is_open:
            raise Exception("Transaction is not open")

        from gremlin_python.driver.driver_remote_connection import (
            DriverRemoteConnection,
        )

        request_options = DriverRemoteConnection.extract_request_options(gremlin_lang)
        request_options["transactionId"] = self._transaction.transaction_id

        result_set = await self._transaction._client.submit(
            gremlin_lang.get_gremlin(), request_options=request_options
        )
        return RemoteTraversal(result_set)

    submit_async = submit

    def is_closed(self) -> bool:
        return not self._transaction.is_open
