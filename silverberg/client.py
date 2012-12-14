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

from silverberg.cassandra import Cassandra
from silverberg.cassandra import ttypes

from silverberg.marshal import prepare, unmarshallers

import re

from silverberg.thrift_client import OnDemandThriftClient

# used to parse the CF name out of a select statement.
selectRe = re.compile(r"\s*SELECT\s+.+\s+FROM\s+[\']?(\w+)", re.I | re.M)



class CassandraClient(object):
    def __init__(self, cass_endpoint, keyspace, user=None, password=None):
        self._client = OnDemandThriftClient(cass_endpoint, Cassandra.Client)

        self._keyspace = keyspace
        self._user = user
        self._password = password
        self._validators = {}

    def _learn(self, client):
        def _learn(keyspaceDef):
            for cf_def in keyspaceDef.cf_defs:
                sp_val = {}
                for col_meta in cf_def.column_metadata:
                    sp_val[col_meta.name] = col_meta.validation_class
                self._validators[cf_def.name] = {
                    "key": cf_def.key_validation_class,
                    "comparator": cf_def.comparator_type,
                    "defaultValidator": cf_def.default_validation_class,
                    "specific_validators": sp_val
                }
            return client
        d = client.describe_keyspace(self._keyspace)
        return d.addCallback(_learn)

    def _set_keyspace(self, client):
        d = client.set_keyspace(self._keyspace)
        return d.addCallback(lambda _: client)

    def _login(self, client):
        creds = {'user': self._user, 'password': self._password}
        authreq = ttypes.AuthenticationRequest(creds)
        d = client.login(authreq)
        d.addCallback(lambda _: client)
        return d

    def _get_client(self):
        d = self._client.get_client()
        if self._user and self._password:
            d.addCallback(self._login)

        d.addCallback(self._set_keyspace)
        d.addCallback(self._learn)
        return d

    def describe_version(self):
        def _vers(client):
            return client.describe_version()

        d = self._get_client()
        d.addCallback(_vers)
        return d

    def _unmarshal_result(self, cfname, raw_rows):
        rows = []
        if cfname not in self._validators:
            validator = None
        else:
            validator = self._validators[cfname]

        def _unmarshal_val(type, val):
            if type is None:
                return val
            elif type in unmarshallers:
                return unmarshallers[type](val)
            else:
                return val

        def _find_specific(col):
            if validator is None:
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
                key = _unmarshal_val(validator['key'], raw_row.key)
            for raw_col in raw_row.columns:
                specific = _find_specific(raw_col.name)
                temp_col = {"timestamp": raw_col.timestamp,
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
            return client.execute_cql_query(prepare(
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
