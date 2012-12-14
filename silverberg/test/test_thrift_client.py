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

from twisted.internet.defer import Deferred
from twisted.internet.error import ConnectError, ConnectionLost, ConnectionDone

from twisted.python.failure import Failure

from silverberg.thrift_client import OnDemandThriftClient
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
        self.client_proto = mock.Mock()

        def _connect(factory):
            self.connect_d = Deferred()
            wrapper = mock.Mock()
            wrapper.wrapped.client = self.client_proto
            return self.connect_d.addCallback(lambda _: wrapper)

        self.endpoint.connect.side_effect = _connect

        self.client = OnDemandThriftClient(self.endpoint, mock.Mock())

    def test_initial_connect(self):
        d = self.client.get_client()

        self.connect_d.callback(None)

        self.assertEqual(self.assertFired(d), self.client_proto)

    def test_connected(self):
        d1 = self.client.get_client()

        self.connect_d.callback(None)

        d2 = self.client.get_client()

        self.assertNotIdentical(d1, d2)

        self.assertEqual(self.assertFired(d2), self.client_proto)

    def test_connect_while_connecting(self):
        d1 = self.client.get_client()

        d2 = self.client.get_client()

        self.assertNotIdentical(d1, d2)

        self.connect_d.callback(None)

        self.assertEqual(self.assertFired(d1), self.client_proto)
        self.assertEqual(self.assertFired(d2), self.client_proto)

    def test_connect_failed(self):
        d = self.client.get_client()

        self.connect_d.errback(_TestConnectError())

        f = self.assertFailed(d)

        self.assertTrue(f.check(_TestConnectError))

    def test_connection_lost_cleanly(self):
        d = self.client.get_client()

        self.connect_d.callback(None)
        self.assertFired(d)

        self.connection_lost(Failure(_TestConnectionDone()))

    def test_connection_lost_uncleanly(self):
        d = self.client.get_client()

        self.connect_d.callback(None)

        self.assertFired(d)

        self.connection_lost(Failure(_TestConnectionLost()))

        self.flushLoggedErrors(_TestConnectionLost)
