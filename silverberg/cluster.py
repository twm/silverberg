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

from silverberg.client import CQLClient


class RoundRobinCassandraCluster(object):
    """
    Maintain several :py:class:`silverberg.client.CQLClient` instances
    connected `seed_endpoints` using `keyspace`.  Each time :py:func:`execute`
    is called a client will be selected in a round-robin fashion.

    :param seed_endpoints: A list of `IStreamClientEndpoint` providers to maintain
        client connections to.
    :param str keyspace: The cassandra keyspace to use.

    :param str user: Optional username.
    :param str password: Optional password.
    """
    def __init__(self, seed_endpoints, keyspace, user=None, password=None):
        self._seed_clients = [
            CQLClient(endpoint, keyspace, user, password)
            for endpoint in seed_endpoints
        ]
        self._client_idx = 0

    def _client(self):
        n = self._client_idx % len(self._seed_clients)
        client = self._seed_clients[n]
        self._client_idx += 1
        return client

    def execute(self, *args, **kwargs):
        """
        See :py:func:`silverberg.client.CQLClient.execute`
        """
        return self._client().execute(*args, **kwargs)
