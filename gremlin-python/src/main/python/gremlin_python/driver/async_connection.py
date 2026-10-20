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

from gremlin_python.driver import useragent
from gremlin_python.driver.aiohttp.async_transport import AsyncAiohttpHTTPTransport
from gremlin_python.driver.async_resultset import AsyncResultSet, _STREAM_END
from gremlin_python.driver.connection import (
    GremlinConnectionError,
    GremlinServerError,
    _CONNECTION_ERROR_MSG,
    _TRANSPORT_ERRORS,
)
from gremlin_python.driver.http_request import HttpRequest
from gremlin_python.driver.serializer import GraphBinarySerializersV4


class AsyncConnection:
    """Native ``asyncio`` counterpart to :class:`~gremlin_python.driver.connection.Connection`
    for TinkerPop 4.0 HTTP streaming.

    Encapsulates a single :class:`~gremlin_python.driver.aiohttp.async_transport.AsyncAiohttpHTTPTransport`
    instance, executes synchronous or asynchronous request interceptors, and
    streams deserialized ``GraphBinaryV4`` items into an :class:`AsyncResultSet`.
    """

    def __init__(
        self,
        url,
        traversal_source,
        pool: asyncio.Queue,
        response_serializer=None,
        auth=None,
        interceptors=None,
        enable_user_agent_on_connect=True,
        bulk_results=False,
        transport_factory=None,
        headers=None,
        **transport_kwargs,
    ):
        if callable(interceptors):
            interceptors = [interceptors]
        elif isinstance(interceptors, tuple):
            interceptors = list(interceptors)
        elif not (isinstance(interceptors, list) or interceptors is None):
            raise TypeError("interceptors must be a callable, tuple, list or None")

        if auth is not None:
            interceptors = (interceptors or []) + [auth]

        self._url = url
        self._headers = dict(headers) if isinstance(headers, dict) else (list(headers) if isinstance(headers, list) else None)
        self._traversal_source = traversal_source
        self._transport_factory = transport_factory
        self._transport_kwargs = transport_kwargs
        self._transport = None
        self._pool = pool
        self._result_set = None
        self._inited = False
        self._response_serializer = response_serializer or GraphBinarySerializersV4()
        self._interceptors = interceptors
        self._enable_user_agent_on_connect = enable_user_agent_on_connect
        if self._enable_user_agent_on_connect:
            self.__add_header(useragent.userAgentHeader, useragent.userAgent)
        self._bulk_results = bulk_results
        if self._bulk_results:
            self.__add_header("bulkResults", "true")

    async def connect(self):
        if self._transport is not None:
            await self._transport.close()
        if self._transport_factory is not None:
            self._transport = self._transport_factory()
        else:
            self._transport = AsyncAiohttpHTTPTransport(**self._transport_kwargs)
        await self._transport.connect(self._url, self._headers)
        self._inited = True

    async def close(self):
        if self._inited and self._transport is not None:
            await self._transport.close()
            self._inited = False

    async def _write_request(self, request_message):
        accept = str(self._response_serializer.version, encoding="utf-8")
        headers = {"accept": accept}

        # Promote transactionId to HTTP header before interceptors run.
        # The field remains in the serialized body as well (dual transmission
        # per the HTTP transaction protocol specification).
        if hasattr(request_message, "fields") and "transactionId" in request_message.fields:
            headers["X-Transaction-Id"] = request_message.fields["transactionId"]

        http_request = HttpRequest(
            method="POST",
            url=self._url,
            headers=headers,
            body=request_message,
        )

        for interceptor in self._interceptors or []:
            res = interceptor(http_request)
            if inspect.isawaitable(res):
                await res

        # Auto-serialize if no interceptor already did so
        http_request.serialize_body()

        message = {
            "headers": http_request.headers,
            "payload": http_request.body,
        }
        await self._transport.write(message)

    async def write(self, request_message) -> AsyncResultSet:
        if not self._inited:
            try:
                await self.connect()
            except _TRANSPORT_ERRORS as e:
                self._pool.put_nowait(self)
                raise GremlinConnectionError(_CONNECTION_ERROR_MSG) from e
            except BaseException:
                self._pool.put_nowait(self)
                raise

        self._result_set = AsyncResultSet(asyncio.Queue())
        try:
            await self._write_request(request_message)
        except _TRANSPORT_ERRORS as e:
            self._pool.put_nowait(self)
            raise GremlinConnectionError(_CONNECTION_ERROR_MSG) from e
        except BaseException:
            self._pool.put_nowait(self)
            raise

        self._result_set.done = asyncio.create_task(self._receive(self._result_set))
        return self._result_set

    async def _receive(self, result_set: AsyncResultSet = None):
        if result_set is None:
            result_set = self._result_set
        try:
            status = getattr(self._transport, "status_code", None)
            response_content_type = str(self._response_serializer.version, encoding="utf-8")
            if status is not None and status >= 400:
                content_type = getattr(self._transport, "content_type", "")
                if response_content_type not in content_type:
                    raw = await self._transport.read_body()
                    body = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
                    raise GremlinServerError({
                        "code": status,
                        "message": body,
                        "exception": "",
                    })

            # 204 No Content
            if status == 204:
                return

            stream = self._transport.get_stream()
            async for obj in self._response_serializer.deserialize_async_response_stream(stream):
                result_set.stream.put_nowait(obj)
        except _TRANSPORT_ERRORS as err:
            self._transport.evict_response()
            msg = (
                "Server returned an empty response body"
                if isinstance(err, asyncio.IncompleteReadError) and not err.partial
                else _CONNECTION_ERROR_MSG
            )
            raise GremlinConnectionError(msg) from err
        except Exception:
            self._transport.evict_response()
            raise
        finally:
            if result_set is not None:
                result_set.stream.put_nowait(_STREAM_END)
            self._pool.put_nowait(self)

    def __add_header(self, key, value):
        if self._headers is None:
            self._headers = dict()
        if isinstance(self._headers, list):
            for pair in list(self._headers):
                if pair[0] == key:
                    self._headers.remove(pair)
            self._headers.append((key, value))
        else:
            self._headers[key] = value
