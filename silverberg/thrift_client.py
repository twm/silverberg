# Copyright 2012 Rackspace Hosting, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import deque

from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol

from twisted.internet.error import ConnectionDone
from twisted.internet.defer import succeed, Deferred
from twisted.internet.protocol import Protocol, Factory

from twisted.python import log
from twisted.python.constants import NamedConstant, Names


class _LossNotifyingWrapperProtocol(Protocol):
    def __init__(self, wrapped, on_connectionLost):
        self.wrapped = wrapped
        self._on_connectionLost = on_connectionLost

    def dataReceived(self, data):
        self.wrapped.dataReceived(data)

    def connectionLost(self, reason):
        self.wrapped.connectionLost(reason)
        self._on_connectionLost(reason)

    def connectionMade(self):
        self.wrapped.makeConnection(self.transport)


class _ThriftClientFactory(Factory):
    def __init__(self, client_class, on_connectionLost):
        self._client_class = client_class
        self._on_connectionLost = on_connectionLost

    def buildProtocol(self, addr):
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        p = TTwisted.ThriftClientProtocol(self._client_class, pfactory)

        wrapper = _LossNotifyingWrapperProtocol(
            p, self._on_connectionLost)

        return wrapper


class _State(Names):
    NOT_CONNECTED = NamedConstant()
    CONNECTING = NamedConstant()
    CONNECTED = NamedConstant()


class OnDemandThriftClient(object):
    def __init__(self, endpoint, client_class):
        self._endpoint = endpoint
        self._factory = _ThriftClientFactory(client_class,
                                             self._connection_lost)

        self._state = _State.NOT_CONNECTED
        self._current_client = None
        self._waiting_on_connect = deque()

    def _notify_on_connect(self):
        d = Deferred()
        self._waiting_on_connect.append(d)
        return d

    def _connection_lost(self, reason):
        self._state = _State.NOT_CONNECTED

        if not reason.check(ConnectionDone):
            log.err(reason,
                    "Lost current connection, reconnecting on demand.",
                    system=self.__class__.__name__)

    def _connection_made(self, wrapper):
        self._state = _State.CONNECTED
        self._current_client = wrapper.wrapped.client

        # XXX: Is the above state change sufficient to deal with re-entrancy?

        while self._waiting_on_connect:
            d = self._waiting_on_connect.popleft()
            d.callback(self._current_client)

        return self._current_client

    def _connection_failed(self, reason):
        self._state = _State.NOT_CONNECTED
        self._current_client = None

        # XXX: Is the above state change sufficient to deal with re-entrancy?

        while self._waiting_on_connect:
            d = self._waiting_on_connect.popleft()
            d.errback(reason)

        return reason

    def _connect(self):
        self._state = _State.CONNECTING

        d = self._endpoint.connect(self._factory)
        d.addCallbacks(self._connection_made, self._connection_failed)

        return d

    def get_client(self):
        if self._state == _State.CONNECTED:
            return succeed(self._current_client)
        elif self._state == _State.NOT_CONNECTED:
            d = self._notify_on_connect()
            self._connect()
            return d
        else:
            assert self._state == _State.CONNECTING
            return self._notify_on_connect()
