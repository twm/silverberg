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

import mock

from twisted.internet.defer import succeed, Deferred
from twisted.internet.protocol import Protocol
from twisted.internet.error import ConnectError, ConnectionLost, ConnectionDone

from twisted.python.failure import Failure

from silverberg.thrift_client import (
    OnDemandThriftClient,
    ClientDisconnecting,
    ClientConnecting,
    _LossNotifyingWrapperProtocol,
    _ThriftClientFactory
)

from silverberg.cassandra.Cassandra import Client
from silverberg.test.util import BaseTestCase


class _TestConnectError(ConnectError):
    pass


class _TestConnectionDone(ConnectionDone):
    pass


class _TestConnectionLost(ConnectionLost):
    pass


class OnDemandThriftClientTests(BaseTestCase):
    def setUp(self):
        self.factory_patcher = mock.patch('silverberg.thrift_client._ThriftClientFactory')

        self.factory = self.factory_patcher.start()

        def _create_factory(client_class, connection_lost):
            self.connection_lost = connection_lost

        self.factory.side_effect = _create_factory

        self.addCleanup(self.factory_patcher.stop)

        self.endpoint = mock.Mock()
        self.twisted_transport = mock.Mock()
        self.thrift_cassandra_client = mock.Mock(Client)

        def _connect(factory):
            self.connect_d = Deferred()
            wrapper = mock.Mock()
            wrapper.transport = self.twisted_transport
            wrapper.wrapped.client = self.thrift_cassandra_client
            return self.connect_d.addCallback(lambda _: wrapper)

        self.endpoint.connect.side_effect = _connect

        self.client = OnDemandThriftClient(self.endpoint, mock.Mock())

    def test_initial_connect(self):
        d = self.client.connection()

        self.connect_d.callback(None)

        self.assertEqual(self.assertFired(d), self.thrift_cassandra_client)

    def test_initial_handshake_non_deferred(self):
        def _mock_handshake(client):
            return None

        m = mock.MagicMock(side_effect=_mock_handshake)

        d = self.client.connection(m)

        self.connect_d.callback(None)

        self.assertEqual(self.assertFired(d), self.thrift_cassandra_client)
        m.assert_called_once()

    def test_initial_handshake_deferred(self):
        def _mock_handshake_d(client):
            return succeed(None)

        m = mock.MagicMock(side_effect=_mock_handshake_d)

        d = self.client.connection(m)
        self.connect_d.callback(None)

        self.assertEqual(self.assertFired(d), self.thrift_cassandra_client)
        m.assert_called_once()

    def test_connected(self):
        d1 = self.client.connection()

        self.connect_d.callback(None)

        d2 = self.client.connection()

        self.assertNotIdentical(d1, d2)

        self.assertEqual(self.assertFired(d2), self.thrift_cassandra_client)

    def test_connect_while_connecting(self):
        d1 = self.client.connection()

        d2 = self.client.connection()

        self.assertNotIdentical(d1, d2)

        self.connect_d.callback(None)

        self.assertEqual(self.assertFired(d1), self.thrift_cassandra_client)
        self.assertEqual(self.assertFired(d2), self.thrift_cassandra_client)

    def test_connect_failed(self):
        d = self.client.connection()

        self.connect_d.errback(_TestConnectError())

        self.assertFailed(d, _TestConnectError)

    def test_connection_lost_cleanly(self):
        d = self.client.connection()

        self.connect_d.callback(None)
        self.assertFired(d)

        self.connection_lost(Failure(_TestConnectionDone()))

    @mock.patch('silverberg.thrift_client.log')
    def test_connection_lost_uncleanly(self, mock_log):
        self.twisted_transport.getPeer.return_value = 'peer addr'
        d = self.client.connection()

        self.connect_d.callback(None)

        self.assertFired(d)

        reason = Failure(_TestConnectionLost())
        self.connection_lost(reason)

        mock_log.err.assert_called_once_with(
            reason, "Lost current connection to 'peer addr', reconnecting on demand.",
            system='OnDemandThriftClient', node='peer addr')

    def test_disconnect(self):
        d1 = self.client.connection()

        self.connect_d.callback(None)

        self.assertFired(d1)

        d2 = self.client.disconnect()

        self.twisted_transport.loseConnection.assert_called_once()

        self.connection_lost(Failure(_TestConnectionDone()))

        self.assertFired(d2)

    def test_connect_while_disconnecting(self):
        d1 = self.client.connection()

        self.connect_d.callback(None)

        self.assertFired(d1)

        d2 = self.client.disconnect()

        d3 = self.client.connection()

        self.connection_lost(Failure(_TestConnectionDone()))

        self.assertFired(d2)

        self.assertFailed(d3, ClientDisconnecting)

    def test_disconnect_while_connecting(self):
        d1 = self.client.connection()
        d2 = self.client.disconnect()

        self.assertFailed(d2, ClientConnecting)
        self.assertEqual(
            len(self.twisted_transport.loseConnection.mock_calls), 0,
            "loseConnection should not be called since not connected")

        self.connect_d.callback(None)

        self.assertFired(d1)

    def test_disconnect_while_disconnecting(self):
        self.client.connection()
        self.connect_d.callback(None)

        d1 = self.client.disconnect()
        self.twisted_transport.reset_mock()
        d2 = self.client.disconnect()

        self.connection_lost(Failure(_TestConnectionDone()))
        self.assertEqual(
            len(self.twisted_transport.loseConnection.mock_calls), 0,
            "loseConnection should not be called since not connected")

        self.assertFired(d1)
        self.assertFired(d2)

    def test_disconnect_while_not_connected(self):
        d1 = self.client.disconnect()
        self.assertIdentical(self.assertFired(d1), None)
        self.assertEqual(
            len(self.twisted_transport.loseConnection.mock_calls), 0,
            "loseConnection should not be called since not connected")


class LossNotifyingWrapperProtocolTests(BaseTestCase):
    def setUp(self):
        self.wrapped = mock.Mock(Protocol)

        self.connection_lost = mock.Mock()

        self.wrapper = _LossNotifyingWrapperProtocol(
            self.wrapped,
            self.connection_lost
        )

    def test_makeConnection(self):
        transport = mock.Mock()
        self.wrapper.makeConnection(transport)
        self.wrapped.makeConnection.assert_called_once_with(transport)

    def test_dataReceived(self):
        self.wrapper.dataReceived('foo')
        self.wrapped.dataReceived.assert_called_once_with('foo')

    def test_connectionLost(self):
        reason = Failure(_TestConnectionDone())
        self.wrapper.connectionLost(reason)
        self.wrapped.connectionLost.assert_called_once_with(reason)


class ThriftClientFactoryTests(BaseTestCase):
    def setUp(self):
        self.client_class = mock.Mock()
        self.connection_lost = mock.Mock()
        self.factory = _ThriftClientFactory(self.client_class, self.connection_lost)

    def test_buildProtocol(self):
        protocol = self.factory.buildProtocol(mock.Mock())

        self.assertIsInstance(protocol, _LossNotifyingWrapperProtocol)
