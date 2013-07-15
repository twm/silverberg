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

from silverberg.test.util import BaseTestCase

from silverberg.client import CQLClient

from silverberg.cluster import RoundRobinCassandraCluster


class RoundRobinCassandraClusterTests(BaseTestCase):
    def setUp(self):
        self.cql_client_patcher = mock.patch("silverberg.cluster.CQLClient")
        self.CQLClient = self.cql_client_patcher.start()
        self.addCleanup(self.cql_client_patcher.stop)

        self.clients = []

        def _CQLClient(*args, **kwargs):
            c = mock.Mock(CQLClient)
            self.clients.append(c)
            return c

        self.CQLClient.side_effect = _CQLClient

    def test_round_robin_execute(self):
        cluster = RoundRobinCassandraCluster(['one', 'two', 'three'], 'keyspace')

        for client, arg in [(1, 'foo'), (2, 'bar'), (0, 'baz'), (1, 'bax')]:
            result = cluster.execute(arg)
            self.clients[client].execute.assert_called_with(arg)
            self.assertEqual(self.clients[client].execute.return_value, result)

    def test_disconnect(self):
        cluster = RoundRobinCassandraCluster(['one', 'two', 'three'], 'keyspace')
        cluster.disconnect()

        for client in self.clients:
            client.disconnect.assert_called_with()
