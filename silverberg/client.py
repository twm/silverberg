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

from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol

from twisted.python import log

from twisted.internet.protocol import Factory, Protocol
from twisted.internet.defer import Deferred, succeed

from silverberg.cassandra import Cassandra
from silverberg.cassandra import ttypes

from silverberg.marshal import prepare, unmarshallers

import re

# used to parse the CF name out of a select statement.
selectRe = re.compile(r"\s*SELECT\s+.+\s+FROM\s+[\']?(\w+)",re.I | re.M)

class _LossNotifyingWrapperProtocol(Protocol):
    def __init__(self, wrapped, on_connectionLost):
        self.wrapped = wrapped
        self._on_connectionLost = on_connectionLost

    def dataReceived(self, data):
        self.wrapped.dataReceived(data)

    def connectionLost(self, reason):
        self.wrapped.connectionLost(reason)
        self._on_connectionLost(reason)

    def connectionMade(self):
        self.wrapped.makeConnection(self.transport)


class _ThriftClientFactory(Factory):
    def __init__(self, client_class, on_connectionLost):
        self._client_class = client_class
        self._on_connectionLost = on_connectionLost

    def buildProtocol(self, addr):
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        p = TTwisted.ThriftClientProtocol(self._client_class, pfactory)

        wrapper = _LossNotifyingWrapperProtocol(
            p, self._on_connectionLost)

        return wrapper


class CassandraClient(object):
    _state = 'NOT_CONNECTED'

    def __init__(self, cass_endpoint, keyspace, user=None, password=None):
        self._cass_endpoint = cass_endpoint
        self._client_factory = _ThriftClientFactory(
            Cassandra.Client,
            self._connection_lost)
        self._client = None
        self._waiting = []
        self._keyspace = keyspace
        self._user = user
        self._password = password
        self._validators = {}
        
    def _notify_connected(self):
        d = Deferred()
        self._waiting.append(d)
        return d

    def _connection_learn(self, client):
        def _learn(keyspaceDef):
            for cf_def in keyspaceDef.cf_defs:
                specific_validators = {}
                for col_meta in cf_def.column_metadata:
                    specific_validators[col_meta.name] = col_meta.validation_class
                self._validators[cf_def.name] = {
                "key": cf_def.key_validation_class,
                "comparator": cf_def.comparator_type,
                "defaultValidator": cf_def.default_validation_class,
                "specific_validators": specific_validators
                }
            return client
        d = self._client.client.describe_keyspace(self._keyspace)
        return d.addCallback(_learn)

    def _connection_set_keyspace(self, client):
        d = self._client.client.set_keyspace(self._keyspace)
        return d.addCallback(lambda _: client)

    def _connection_login(self, client):
        creds = {'user': self._user, 'password': self._password}
        authreq = ttypes.AuthenticationRequest(creds)
        d = self._client.client.login(authreq)
        return d.addCallback(lambda _: client)

    def _connection_ready(self, client):
        self._client = client.wrapped
        return self._client

    def _connection_made(self, client):
        self._state = 'CONNECTED'
        while self._waiting:
            d = self._waiting.pop(0)
            d.callback(self._client)
        return self._client

    def _connection_lost(self, reason):
        self._state = 'NOT_CONNECTED'
        self._client = None
        log.err(
            reason,
            "Connection lost to cassandra server: {0}".format(
                self._cass_endpoint))

    def _connection_failed(self, reason):
        self._state = 'NOT_CONNECTED'
        self._client = None
        log.err(
            reason,
            "Could not connect to cassandra server: {0}".format(
                self._cass_endpoint))
        return reason

    def _get_client(self):
        if self._state == 'NOT_CONNECTED':
            self._state = 'CONNECTING'
            d = self._cass_endpoint.connect(self._client_factory)
            d.addCallbacks(self._connection_ready, self._connection_failed)
            if self._user is not None:
                d.addCallback(self._connection_login)
            d.addCallback(self._connection_learn)
            d.addCallback(self._connection_set_keyspace)
            d.addCallback(self._connection_made)
            return d
        elif self._state == 'CONNECTING':
            return self._notify_connected()
        elif self._state == 'CONNECTED':
            return succeed(self._client)

    def describe_version(self):
        def _vers(client):
            return client.client.describe_version()

        d = self._get_client()
        d.addCallback(_vers)
        return d
        
    def _unmarshal_result(self, cfname, raw_rows):
        rows = []
        if cfname not in self._validators:
            validator = None;
        else:
            validator = self._validators[cfname]

        def _unmarshal_val(type, val):
            if type == None:
                return val
            elif type in unmarshallers:
                return unmarshallers[type](val)
            else:
                return val
                
        def _find_specific(col):
            if validator == None:
                return None
            elif col in validator['specific_validators']:
                return validator['specific_validators'][col]
            else:
                return validator['defaultValidator']
                
        for raw_row in raw_rows:
            cols = []
            #as it turns out, you can have multiple cols with the same
            #name, ergo, we're passing back an array instead of a hash
            #keyed by key name
            key = raw_row.key
            if validator is not None:
                key = _unmarshal_val(validator['key'],raw_row.key)
            for raw_col in raw_row.columns:
                specific = _find_specific(raw_col.name)
                temp_col = {"timestamp" : raw_col.timestamp, 
                            "name": raw_col.name,
                            "ttl": raw_col.ttl,
                            "value": _unmarshal_val(specific, raw_col.value)}
                cols.append(temp_col)
            rows.append(
                {"key": key, "cols": cols}
            )
        return rows
        
    def execute(self, query, args):
        def _execute(client):
            return client.client.execute_cql_query(prepare(
                query, args), ttypes.Compression.NONE)

        def _proc_results(result):
            if result.type == ttypes.CqlResultType.ROWS:
                cfname = selectRe.match(query).group(1)
                return self._unmarshal_result(cfname, result.rows)
            elif result.type == ttypes.CqlResultType.INT:
                return result.num
            else:
                return None

        d = self._get_client()
        d.addCallback(_execute)
        d.addCallback(_proc_results)
        return d
