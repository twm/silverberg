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

from silverberg.client import CassandraClient

from silverberg.cluster import RoundRobinCassandraCluster


class RoundRobinCassandraClusterTests(BaseTestCase):
    def setUp(self):
        self.CassandraClient = mock.Mock(CassandraClient)

        self.clients = []

        def _CassandraClient(*args, **kwargs):
            c = mock.Mock(CassandraClient)
            self.clients.append(c)
            return c

        self.CassandraClient.side_effect = _CassandraClient

    def test_getattr(self):
        cluster = RoundRobinCassandraCluster(
            ['one'], 'keyspace',
            _client_class=self.CassandraClient)

        self.CassandraClient.assert_called_once_with('one', 'keyspace', None, None)

        result = cluster.execute('foo', 'bar')

        self.clients[0].execute.assert_called_once_with('foo', 'bar')
        self.assertEqual(self.clients[0].execute.return_value, result)

    def test_getattr_fails(self):
        cluster = RoundRobinCassandraCluster(
            ['one'], 'keyspace',
            _client_class=self.CassandraClient)

        def _try_access_unknown():
            cluster.unknown_method()

        self.assertRaises(AttributeError, _try_access_unknown)

    def test_round_robin(self):
        cluster = RoundRobinCassandraCluster(
            ['one', 'two', 'three'], 'keyspace',
            _client_class=self.CassandraClient)

        for client, arg in [(0, 'foo'), (1, 'bar'), (2, 'baz'), (0, 'bax')]:
            result = cluster.execute(arg)
            self.clients[client].execute.assert_called_with(arg)
            self.assertEqual(self.clients[client].execute.return_value, result)

