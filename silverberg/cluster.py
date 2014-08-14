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

from twisted.internet.error import ConnectError


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
    :param bool disconnect_on_cancel: Should TCP connection be disconnected on
                cancellation of running query? Defaults to False
    """

    def __init__(self, seed_endpoints, keyspace, user=None, password=None,
                 disconnect_on_cancel=False):
        self._seed_clients = [
            CQLClient(endpoint, keyspace, user, password, disconnect_on_cancel)
            for endpoint in seed_endpoints
        ]
        self._client_idx = 0

    def execute(self, *args, **kwargs):
        """
        See :py:func:`silverberg.client.CQLClient.execute`
        """
        num_clients = len(self._seed_clients)
        start_client = (self._client_idx + 1) % num_clients

        def _client_error(failure, client_i):
            failure.trap(ConnectError)
            client_i = (client_i + 1) % num_clients
            if client_i == start_client:
                return failure
            else:
                return _try_execute(client_i)

        def _try_execute(client_i):
            self._client_idx = client_i
            d = self._seed_clients[client_i].execute(*args, **kwargs)
            return d.addErrback(_client_error, client_i)

        return _try_execute(start_client)

    def disconnect(self):
        """
        Disconnect from the cassandra cluster.  Cassandara and Silverberg do
        not require the connection to be closed before exiting.  However, this
        method may be useful if resources are constrained, or for testing
        purposes.

        :return: a :class:`DeferredList` that fires with a list of None's when every client
        has disconnected.
        """
        return DeferredList([client.disconnect() for client in self._seed_clients])
