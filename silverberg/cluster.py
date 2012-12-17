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

from silverberg.client import CassandraClient


class RoundRobinCassandraCluster(object):
    def __init__(self, seed_endpoints, keyspace, user=None, password=None):
        self._seed_clients = [
            CassandraClient(endpoint, keyspace, user, password)
            for endpoint in seed_endpoints
        ]
        self._client_idx = 0

    def _client(self):
        n = self._client_idx % len(self._seed_clients)
        client = self._seed_clients[n]
        self._client_idx += 1
        return client

    def execute(self, *args, **kwargs):
        return self._client().execute(*args, **kwargs)
