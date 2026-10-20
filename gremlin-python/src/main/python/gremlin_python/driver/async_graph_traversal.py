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
import inspect
import logging
import warnings

from gremlin_python.process.graph_traversal import GraphTraversal, GraphTraversalSource
from gremlin_python.process.traversal import (
    GremlinLang,
    Traversal,
    TraversalStrategies,
    TraversalStrategy,
    Traverser,
)
from gremlin_python.structure.graph import Graph

log = logging.getLogger("gremlinpython")


class AsyncRemoteStrategy(TraversalStrategy):
    """Async counterpart to :class:`~gremlin_python.driver.remote_connection.RemoteStrategy`.

    Submits ``traversal.gremlin_lang`` to the configured
    :class:`~gremlin_python.driver.async_driver_remote_connection.AsyncDriverRemoteConnection`
    and attaches the resulting :class:`~gremlin_python.driver.async_resultset.AsyncResultSet`
    to ``traversal.traversers``.
    """

    def __init__(self, remote_connection):
        TraversalStrategy.__init__(self, fqcn="py:RemoteStrategy")
        self.remote_connection = remote_connection

    async def apply(self, traversal):
        if traversal.traversers is None:
            remote_traversal = await self.remote_connection.submit(traversal.gremlin_lang)
            traversal.remote_results = remote_traversal
            traversal.traversers = remote_traversal.traversers

    apply_async = apply


class AsyncTraversalStrategies(TraversalStrategies):
    """Async counterpart to :class:`~gremlin_python.process.traversal.TraversalStrategies`."""

    async def apply_strategies(self, traversal):
        for traversal_strategy in self.traversal_strategies:
            res = traversal_strategy.apply(traversal)
            if inspect.isawaitable(res):
                await res

    apply_async_strategies = apply_strategies


class AsyncTraversal(Traversal):
    """Async counterpart to :class:`~gremlin_python.process.traversal.Traversal`
    for TinkerPop 4.0.

    All terminal steps (``to_list()``, ``to_set()``, ``next()``, ``iterate()``,
    ``has_next()``, ``next_traverser()``) and iteration (``async for``) are
    native coroutines that unroll bulked :class:`Traverser` instances directly
    from the streaming :class:`~gremlin_python.driver.async_resultset.AsyncResultSet`.
    """

    def __iter__(self):
        raise NotImplementedError(
            "AsyncTraversal does not support synchronous iteration. "
            "Use 'async for item in traversal' or 'await traversal.to_list()'."
        )

    def __next__(self):
        raise NotImplementedError(
            "AsyncTraversal does not support synchronous next(). "
            "Use 'await traversal.next()' or 'await traversal.__anext__()'."
        )

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.traversers is None:
            await self.traversal_strategies.apply_strategies(self)
        if self.last_traverser is None:
            self.last_traverser = await self.traversers.__anext__()
        res = self.last_traverser
        if isinstance(res, Traverser):
            obj = res.object
            self.last_traverser.bulk = self.last_traverser.bulk - 1
            if self.last_traverser.bulk <= 0:
                self.last_traverser = None
            return obj
        else:
            self.last_traverser = None
            return res

    async def to_list(self):
        return [item async for item in self]

    async def toList(self):
        warnings.warn(
            "gremlin_python.driver.async_graph_traversal.AsyncTraversal.toList will be replaced by "
            "gremlin_python.driver.async_graph_traversal.AsyncTraversal.to_list.",
            DeprecationWarning,
        )
        return await self.to_list()

    async def to_set(self):
        return {item async for item in self}

    async def toSet(self):
        warnings.warn(
            "gremlin_python.driver.async_graph_traversal.AsyncTraversal.toSet will be replaced by "
            "gremlin_python.driver.async_graph_traversal.AsyncTraversal.to_set.",
            DeprecationWarning,
        )
        return await self.to_set()

    async def iterate(self):
        self.gremlin_lang.add_step("discard")
        while True:
            try:
                await self.next_traverser()
            except StopAsyncIteration:
                return self

    async def next_traverser(self):
        if self.traversers is None:
            await self.traversal_strategies.apply_strategies(self)
        if self.last_traverser is None:
            return await self.traversers.__anext__()
        else:
            temp = self.last_traverser
            self.last_traverser = None
            return temp

    async def nextTraverser(self):
        warnings.warn(
            "gremlin_python.driver.async_graph_traversal.AsyncTraversal.nextTraverser will be replaced by "
            "gremlin_python.driver.async_graph_traversal.AsyncTraversal.next_traverser.",
            DeprecationWarning,
        )
        return await self.next_traverser()

    async def has_next(self) -> bool:
        if self.traversers is None:
            await self.traversal_strategies.apply_strategies(self)
        if self.last_traverser is None:
            try:
                self.last_traverser = await self.traversers.__anext__()
            except StopAsyncIteration:
                return False
        return (
            not (self.last_traverser is None) and self.last_traverser.bulk > 0
            if isinstance(self.last_traverser, Traverser)
            else True
        )

    async def hasNext(self) -> bool:
        warnings.warn(
            "gremlin_python.driver.async_graph_traversal.AsyncTraversal.hasNext will be replaced by "
            "gremlin_python.driver.async_graph_traversal.AsyncTraversal.has_next.",
            DeprecationWarning,
        )
        return await self.has_next()

    async def next(self, amount=None):
        if amount is None:
            try:
                return await self.__anext__()
            except StopAsyncIteration:
                return None
        else:
            count = 0
            temp_list = []
            while count < amount:
                count = count + 1
                try:
                    temp = await self.__anext__()
                except StopAsyncIteration:
                    return temp_list
                temp_list.append(temp)
            return temp_list


class AsyncGraphTraversal(GraphTraversal, AsyncTraversal):
    """Combines all Gremlin DSL steps from :class:`GraphTraversal` with the
    awaitable terminal methods of :class:`AsyncTraversal`."""

    pass


class AsyncGraphTraversalSource(GraphTraversalSource):
    """Async counterpart to :class:`~gremlin_python.process.graph_traversal.GraphTraversalSource`
    that spawns :class:`AsyncGraphTraversal` instances and supports async HTTP
    transactions via :meth:`tx` and :meth:`execute_in_tx`."""

    def __init__(self, graph, traversal_strategies, gremlin_lang=None, remote_connection=None):
        log.info("Creating AsyncGraphTraversalSource.")
        self.graph = graph
        if not isinstance(traversal_strategies, AsyncTraversalStrategies):
            traversal_strategies = AsyncTraversalStrategies(traversal_strategies)
        self.traversal_strategies = traversal_strategies
        if gremlin_lang is None:
            gremlin_lang = GremlinLang()
        self.gremlin_lang = gremlin_lang
        self.graph_traversal = AsyncGraphTraversal
        if remote_connection:
            self.traversal_strategies.add_strategies([AsyncRemoteStrategy(remote_connection)])
            if hasattr(remote_connection, "pdt_registry") and remote_connection.pdt_registry is not None:
                self.gremlin_lang.pdt_registry = remote_connection.pdt_registry
        self.remote_connection = remote_connection

    def get_graph_traversal_source(self):
        return self.__class__(
            self.graph,
            AsyncTraversalStrategies(self.traversal_strategies),
            GremlinLang(self.gremlin_lang),
        )

    def get_graph_traversal(self):
        return self.graph_traversal(
            self.graph,
            self.traversal_strategies,
            GremlinLang(self.gremlin_lang),
        )

    def tx(self):
        """Spawn an :class:`~gremlin_python.driver.async_transaction.AsyncTransaction`
        or return the existing one if this traversal source is already bound to
        a transaction."""
        remote_connection = next(
            (
                x.remote_connection
                for x in self.traversal_strategies.traversal_strategies
                if x.fqcn == "py:RemoteStrategy"
            ),
            None,
        )
        if remote_connection is None:
            raise Exception("Remote connection is required for transactions")

        from gremlin_python.driver.async_transaction import (
            AsyncTransaction,
            AsyncTransactionRemoteConnection,
        )

        if isinstance(remote_connection, AsyncTransactionRemoteConnection):
            return remote_connection._transaction

        return AsyncTransaction(remote_connection._client)

    async def execute_in_tx(self, tx_work):
        """Run an async or sync unit of work inside an HTTP transaction,
        managing its ``begin()`` / ``commit()`` / ``rollback()`` lifecycle."""
        tx = self.tx()
        gtx = await tx.begin()
        try:
            res = tx_work(gtx)
            result = await res if inspect.isawaitable(res) else res
        except BaseException:
            try:
                await tx.rollback()
            except BaseException as rollback_error:
                log.warning("Rollback failed after transaction body error: %s", rollback_error)
            raise
        try:
            await tx.commit()
        except Exception:
            try:
                await tx.rollback()
            except BaseException as rollback_error:
                log.warning("Rollback failed after commit failure: %s", rollback_error)
            raise
        return result


class _AsyncAnonymousTraversalSource:
    """Entry point returned by :func:`async_traversal` to bind an
    :class:`~gremlin_python.driver.async_driver_remote_connection.AsyncDriverRemoteConnection`."""

    def __init__(self, traversal_source_class=AsyncGraphTraversalSource):
        self.traversal_source_class = traversal_source_class

    def with_(self, remote_connection) -> AsyncGraphTraversalSource:
        return self.traversal_source_class(
            Graph(), AsyncTraversalStrategies(), None, remote_connection
        )

    def with_remote(self, remote_connection) -> AsyncGraphTraversalSource:
        return self.with_(remote_connection)

    def withRemote(self, remote_connection) -> AsyncGraphTraversalSource:
        warnings.warn(
            "withRemote will be replaced by with_.",
            DeprecationWarning,
        )
        return self.with_(remote_connection)


def async_traversal(traversal_source_class=AsyncGraphTraversalSource) -> _AsyncAnonymousTraversalSource:
    """Create an anonymous async traversal source to bind with
    ``.with_(AsyncDriverRemoteConnection(...))``."""
    return _AsyncAnonymousTraversalSource(traversal_source_class)
