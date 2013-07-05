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
from twisted.internet import defer, task


class RoundRobinCassandraClusterTests(BaseTestCase):
    """
    Tests for mod:`silverberg.cluster`
    """

    def setUp(self):
        """
        Mock CQLClient
        """
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

    def test_round_robin_execute(self):
        """
        seed_clients are chosen in round robin manner
        """
        cluster = RoundRobinCassandraCluster(['one', 'two', 'three'], 'keyspace')

        for client, arg in [(1, 'foo'), (2, 'bar'), (0, 'baz'), (1, 'bax')]:
            result = cluster.execute(arg)
            self.clients[client].execute.assert_called_with(arg)
            self.assertEqual(self.clients[client].execute.return_value, result)

    def test_one_node_down(self):
        """
        If a cass node is down, it tries the next node in cluster
        """
        cluster = RoundRobinCassandraCluster(['one', 'two', 'three'], 'keyspace')
        self.clients[1].execute.return_value = defer.fail(ConnectionRefusedError())
        result = cluster.execute(2, 3)
        self.assertEqual(self.successResultOf(result), 'exec_ret3')
        self.clients[1].execute.assert_called_once_with(2, 3)
        self.clients[2].execute.assert_called_once_with(2, 3)
        self.assertFalse(self.clients[0].execute.called)

    def test_two_nodes_down(self):
        """
        If 2 cass nodes are down, it tries the next node in cluster
        """
        cluster = RoundRobinCassandraCluster(['one', 'two', 'three'], 'keyspace')
        self.clients[1].execute.return_value = defer.fail(ConnectionRefusedError())
        self.clients[2].execute.return_value = defer.fail(NoRouteError())
        result = cluster.execute(2, 3)
        self.clients[1].execute.assert_called_once_with(2, 3)
        self.clients[2].execute.assert_called_once_with(2, 3)
        self.clients[0].execute.assert_called_once_with(2, 3)
        self.assertEqual(self.successResultOf(result), 'exec_ret1')

    def test_other_error_propogated(self):
        """
        Any error other than subclass of ``ConnectError`` is propgoated
        """
        cluster = RoundRobinCassandraCluster(['one', 'two', 'three'], 'keyspace')
        rand_err = ValueError('random err')
        self.clients[1].execute.return_value = defer.fail(rand_err)
        result = cluster.execute(2, 3)
        self.assertEqual(self.failureResultOf(result).value, rand_err)
        self.assertEqual(self.clients[2].execute.called, False)
        self.assertEqual(self.clients[0].execute.called, False)

    def test_other_error_propogated_on_node_down(self):
        """
        If first node gives ``ConnectError`` then second node is tried if it gives
        error other than subclass of ``ConnectError`` it is propogated
        """
        cluster = RoundRobinCassandraCluster(['one', 'two', 'three'], 'keyspace')
        self.clients[1].execute.return_value = defer.fail(NoRouteError())
        rand_err = ValueError('random err')
        self.clients[2].execute.return_value = defer.fail(rand_err)
        result = cluster.execute(2, 3)
        self.assertEqual(self.failureResultOf(result).value, rand_err)
        self.assertEqual(self.clients[0].execute.called, False)

    def test_all_nodes_down_success_first_node(self):
        """
        If all cass nodes are down, it tries the cluster again after 1 second and succeeds
        """
        clock = task.Clock()
        cluster = RoundRobinCassandraCluster(['one', 'two', 'three'], 'keyspace',
                                             clock=clock)
        self.clients[2].execute.return_value = defer.fail(ConnectionRefusedError())
        self.clients[0].execute.return_value = defer.fail(NoRouteError())

        _execute_first_time = [True]

        def _execute(*args):
            if _execute_first_time[0]:
                _execute_first_time[0] = False
                return defer.fail(ConnectionRefusedError())
            return defer.succeed('execret1')

        self.clients[1].execute.side_effect = _execute

        result = cluster.execute(2, 3)
        clock.advance(1)

        self.assertEqual(self.successResultOf(result), 'execret1')
        self.assertEqual(self.clients[1].execute.mock_calls, [mock.call(2, 3)] * 2)
        self.clients[2].execute.assert_called_once_with(2, 3)
        self.clients[0].execute.assert_called_once_with(2, 3)

    def test_all_nodes_down_max_tries_reached(self):
        """
        If all cass nodes are down consistently, it tries the cluster max_tries times and
        gives up eventually by raising the connection error exception
        """
        clock = task.Clock()
        max_tries = 3
        cluster = RoundRobinCassandraCluster(['one', 'two', 'three'], 'keyspace',
                                             clock=clock, max_tries=max_tries)
        err = ConnectionRefusedError()
        for i in range(3):
            self.clients[i].execute.side_effect = lambda *_: defer.fail(err)

        result = cluster.execute(2, 3)
        clock.pump([1] * max_tries)

        self.assertEqual(self.failureResultOf(result).value, err)
        for i in range(3):
            self.assertEqual(self.clients[i].execute.mock_calls,
                             [mock.call(2, 3)] * max_tries)
