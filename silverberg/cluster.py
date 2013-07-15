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

from twisted.internet.defer import DeferredList

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
        self._client_idx = (self._client_idx + 1) % len(self._seed_clients)
        return self._seed_clients[self._client_idx]

    def execute(self, *args, **kwargs):
        """
        See :py:func:`silverberg.client.CQLClient.execute`
        """
        return self._client().execute(*args, **kwargs)

    def disconnect(self):
        """
        Disconnect every client from the cassandra cluster.  Likely to be used for testing
        purposes only.

        :return: a :class:`DeferredList` that fires with a list of None's when every client
        has disconnected.
        """
        return DeferredList([client.disconnect() for client in self._seed_clients])
