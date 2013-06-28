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

from twisted.internet.error import ConnectionRefusedError, NoRouteError
from twisted.internet import defer


class RoundRobinCassandraClusterTests(BaseTestCase):

    def setUp(self):
        self.cql_client_patcher = mock.patch("silverberg.cluster.CQLClient")
        self.CQLClient = self.cql_client_patcher.start()
        self.addCleanup(self.cql_client_patcher.stop)

        self.clients = []

        def _CQLClient(*args, **kwargs):
            c = mock.Mock(CQLClient)
            self.clients.append(c)
            c.execute.return_value = defer.succeed('exec_ret{}'.format(len(self.clients)))
            return c

        self.CQLClient.side_effect = _CQLClient

        defer_later_patcher = mock.patch('twisted.internet.task.deferLater')
        self.defer_later = defer_later_patcher.start()
        self.addCleanup(defer_later_patcher.stop)
        self.defer_later.return_value = defer.succeed(None)

    def test_round_robin_execute(self):
        cluster = RoundRobinCassandraCluster(['one', 'two', 'three'], 'keyspace')

        for client, arg in [(0, 'foo'), (1, 'bar'), (2, 'baz'), (0, 'bax')]:
            result = cluster.execute(arg)
            self.clients[client].execute.assert_called_with(arg)
            self.assertEqual(self.clients[client].execute.return_value, result)

    def test_one_node_down(self):
        """
        If a cass node is down, it tries the next node in cluster
        """
        cluster = RoundRobinCassandraCluster(['one', 'two', 'three'], 'keyspace')
        self.clients[0].execute.return_value = defer.fail(ConnectionRefusedError())
        result = cluster.execute(2, 3)
        self.clients[0].execute.assert_called_once_with(2, 3)
        self.clients[1].execute.assert_called_once_with(2, 3)
        self.assertFalse(self.clients[2].execute.called)
        self.assertEqual(self.successResultOf(result), 'exec_ret2')

    def test_two_nodes_down(self):
        """
        If 2 cass nodes are down, it tries the next node in cluster
        """
        cluster = RoundRobinCassandraCluster(['one', 'two', 'three'], 'keyspace')
        self.clients[0].execute.return_value = defer.fail(ConnectionRefusedError())
        self.clients[1].execute.return_value = defer.fail(NoRouteError())
        result = cluster.execute(2, 3)
        self.clients[0].execute.assert_called_once_with(2, 3)
        self.clients[1].execute.assert_called_once_with(2, 3)
        self.clients[2].execute.assert_called_once_with(2, 3)
        self.assertEqual(self.successResultOf(result), 'exec_ret3')
