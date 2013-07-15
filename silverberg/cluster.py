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

from twisted.internet.error import ConnectError

from twisted.internet import task


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

    MAX_TRIES = 3       # maximum number of tries to connect to cluster
    TRIES_INTERVAL = 1  # interval between each cluster connection try

    def __init__(self, seed_endpoints, keyspace, user=None, password=None, clock=None,
                 max_tries=None, tries_interval=None):
        self._seed_clients = [
            CQLClient(endpoint, keyspace, user, password)
            for endpoint in seed_endpoints
        ]
        self._client_idx = 0

        if clock:
            self.clock = clock
        else:
            from twisted.internet import reactor
            self.clock = reactor

        self.max_tries = max_tries or self.MAX_TRIES
        self.tries_interval = tries_interval or self.TRIES_INTERVAL

    def execute(self, *args, **kwargs):
        """
        See :py:func:`silverberg.client.CQLClient.execute`
        """
        num_clients = len(self._seed_clients)
        start_client = (self._client_idx + 1) % num_clients

        def _client_error(failure, tries, client_i):
            failure.trap(ConnectError)
            client_i = (client_i + 1) % num_clients
            if client_i == start_client:
                tries += 1
                if tries >= self.max_tries:
                    return failure
                return task.deferLater(self.clock, self.tries_interval,
                                       _try_execute, tries, start_client)
            else:
                return _try_execute(tries, client_i)

        def _try_execute(tries, client_i):
            self._client_idx = client_i
            d = self._seed_clients[client_i].execute(*args, **kwargs)
            return d.addErrback(_client_error, tries, client_i)

        return _try_execute(0, start_client)
