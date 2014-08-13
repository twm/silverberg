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

"""On demand Thrift client."""

from collections import deque

from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol

from twisted.internet.error import ConnectionDone
from twisted.internet.defer import succeed, fail, Deferred, maybeDeferred
from twisted.internet.protocol import Protocol, Factory

from twisted.python import log
from twisted.python.constants import NamedConstant, Names


class ClientDisconnecting(Exception):
    """
    An error that occurs when OnDemandThriftClient.connection() is called
    while the client is in a DISCONNECTING state.

    """


class ClientConnecting(Exception):
    """
    An error that occurs when OnDemandThriftClient.disconnect is called
    while the client is in a CONNECTING state.

    """


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
    DISCONNECTING = NamedConstant()


class OnDemandThriftClient(object):
    """
    Creates a thrift client on-demand.

    When you init the object, it won't actually connect until
    you need it to.

    If the connection is dropped for one reason or another on
    the interim, it'll reconnect when you need it to.

    """
    def __init__(self, endpoint, client_class):
        self._endpoint = endpoint
        self._factory = _ThriftClientFactory(client_class,
                                             self._connection_lost)

        self._state = _State.NOT_CONNECTED
        self._transport = None
        self._current_client = None
        self._waiting_on_connect = deque()
        self._waiting_on_disconnect = deque()

    def _notify_on_disconnect(self):
        d = Deferred()
        self._waiting_on_disconnect.append(d)
        return d

    def _notify_on_connect(self):
        d = Deferred()
        self._waiting_on_connect.append(d)
        return d

    def _connection_lost(self, reason):
        old_state = self._state

        self._state = _State.NOT_CONNECTED

        if old_state == _State.DISCONNECTING:
            while self._waiting_on_disconnect:
                d = self._waiting_on_disconnect.popleft()
                d.callback(None)

        if not reason.check(ConnectionDone):
            node = self._transport.getPeer()
            log.err(reason,
                    "Lost current connection to '{0}', reconnecting on demand.".format(node),
                    system=self.__class__.__name__,
                    node=node)

    def _connection_made(self, client):
        self._state = _State.CONNECTED
        self._current_client = client

        # XXX: Is the above state change sufficient to deal with re-entrancy?

        while self._waiting_on_connect:
            d = self._waiting_on_connect.popleft()
            d.callback(self._current_client)

    def _connection_failed(self, reason):
        self._state = _State.NOT_CONNECTED
        self._current_client = None
        self._transport = None

        # XXX: Is the above state change sufficient to deal with re-entrancy?

        while self._waiting_on_connect:
            d = self._waiting_on_connect.popleft()
            d.errback(reason)

    def _connect(self, handshake):
        self._state = _State.CONNECTING

        def _unwrap_client(wrapper):
            # wrapper in this case is a _LossNotifyingWrapperProtocol, which
            # wraps a thrift.transport.TTwisted.ThriftClientProtocol,
            # whose client is a silverberg.cassandra.Cassandra.Client,
            # whose has an unrelated _transport instance variable that is a
            # thrift.transport.TTwisted.TCallbackTransport, which is a subclass
            # of thrift.transport.TTransport.TTransportBase, whose close method
            # does nothing.

            # so we need to save the _LossNotifyingWrapperProtocol, so that we
            # can disconnect its transport, which is a Twisted ITransport
            # object
            self._transport = wrapper.transport
            return wrapper.wrapped.client

        def _do_handshake(client):
            hd = maybeDeferred(handshake, client)
            hd.addCallback(lambda _: client)
            return hd

        d = self._endpoint.connect(self._factory)
        d.addCallback(_unwrap_client)
        if handshake is not None:
            d.addCallback(_do_handshake)

        d.addCallbacks(self._connection_made, self._connection_failed)

    def connection(self, handshake=None):
        """
        Connects if necessary, returns existing one if it can.

        :param handshake: A function to be called with the client
                          to complete the handshake.

        :returns: thrift connection, deferred if necessary

        """
        if self._state == _State.CONNECTED:
            return succeed(self._current_client)
        elif self._state == _State.DISCONNECTING:
            return fail(ClientDisconnecting())
        elif self._state == _State.NOT_CONNECTED:
            d = self._notify_on_connect()
            self._connect(handshake)
            return d
        else:
            assert self._state == _State.CONNECTING
            return self._notify_on_connect()

    def disconnect(self):
        """
        Disconnects.

        You probably don't need to use this unless you are writing
        unit tests.
        """
        if self._state == _State.CONNECTED:
            self._state = _State.DISCONNECTING
            self._transport.loseConnection()
            return self._notify_on_disconnect()
        if self._state == _State.CONNECTING:
            return fail(ClientConnecting())
        elif self._state == _State.NOT_CONNECTED:
            return succeed(None)
        elif self._state == _State.DISCONNECTING:
            return self._notify_on_disconnect()
