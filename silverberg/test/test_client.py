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

from twisted.internet import defer

from silverberg.client import CQLClient

from silverberg.cassandra import ttypes

from silverberg.test.util import BaseTestCase


class MockClientTests(BaseTestCase):
    def setUp(self):
        self.endpoint = mock.Mock()
        self.client_proto = mock.Mock()

        ksDef = ttypes.KsDef(
            name='blah',
            cf_defs=[ttypes.CfDef(
                comment='',
                key_validation_class='org.apache.cassandra.db.marshal.AsciiType',
                min_compaction_threshold=4,
                key_cache_save_period_in_seconds=None,
                gc_grace_seconds=864000,
                default_validation_class='org.apache.cassandra.db.marshal.UTF8Type',
                max_compaction_threshold=32,
                read_repair_chance=0.1,
                compression_options={
                    'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'},
                bloom_filter_fp_chance=None,
                id=1004,
                keyspace='blah',
                key_cache_size=None,
                replicate_on_write=True,
                subcomparator_type=None,
                merge_shards_chance=None,
                row_cache_provider=None,
                row_cache_save_period_in_seconds=None,
                column_type='Standard',
                memtable_throughput_in_mb=None,
                memtable_flush_after_mins=None,
                column_metadata=[
                    ttypes.ColumnDef(
                        index_type=None,
                        index_name=None,
                        validation_class='org.apache.cassandra.db.marshal.IntegerType',
                        name='fff',
                        index_options=None)],
                key_alias=None,
                dclocal_read_repair_chance=0.0,
                name='blah',
                compaction_strategy_options={},
                row_cache_keys_to_save=None,
                compaction_strategy='org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
                memtable_operations_in_millions=None,
                caching='KEYS_ONLY',
                comparator_type='org.apache.cassandra.db.marshal.UTF8Type',
                row_cache_size=None)],
            strategy_options={'replication_factor': '1'},
            strategy_class='org.apache.cassandra.locator.SimpleStrategy',
            replication_factor=None,
            durable_writes=True)

        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.INT, num=1)

        self.client_proto.set_keyspace.return_value = defer.succeed(None)
        self.client_proto.login.return_value = defer.succeed(None)
        self.client_proto.describe_version.return_value = defer.succeed('1.2.3')
        self.client_proto.describe_keyspace.return_value = defer.succeed(ksDef)

        def _execute_cql_query(*args, **kwargs):
            return defer.succeed(self.mock_results)

        self.client_proto.execute_cql_query.side_effect = _execute_cql_query

        def _connect(factory):
            wrapper = mock.Mock()
            wrapper.wrapped.client = self.client_proto
            return defer.succeed(wrapper)

        self.endpoint.connect.side_effect = _connect

    def test_login(self):
        client = CQLClient(self.endpoint, 'blah', 'groucho', 'swordfish')

        d = client.describe_version()
        self.assertEqual(self.assertFired(d), '1.2.3')
        self.client_proto.describe_version.assert_called_once_with()

        self.client_proto.set_keyspace.assert_called_once_with('blah')

        creds = {'user': 'groucho', 'password': 'swordfish'}
        authreq = ttypes.AuthenticationRequest(creds)
        self.client_proto.login.assert_called_once_with(authreq)

    def test_bad_keyspace(self):
        self.client_proto.set_keyspace.return_value = defer.fail(ttypes.NotFoundException())
        client = CQLClient(self.endpoint, 'blah')

        d = client.describe_version()
        self.assertFailed(d, ttypes.NotFoundException)
        self.client_proto.set_keyspace.assert_called_once_with('blah')

    def test_describe_version(self):
        client = CQLClient(self.endpoint, 'blah')

        d = client.describe_version()
        self.assertEqual(self.assertFired(d), '1.2.3')
        self.assertEqual(self.client_proto.describe_version.call_count, 1)
        self.client_proto.set_keyspace.assert_called_once_with('blah')
        self.client_proto.describe_keyspace.assert_called_once_with('blah')

    def test_cql_value(self):
        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.INT, num=1)
        client = CQLClient(self.endpoint, 'blah')

        d = client.execute("SELECT :sel FROM test_blah", {"sel": "blah"})
        self.assertEqual(self.assertFired(d), 1)
        self.client_proto.execute_cql_query.assert_called_once_with("SELECT 'blah' FROM test_blah", 2)
        self.client_proto.set_keyspace.assert_called_once_with('blah')
        self.client_proto.describe_keyspace.assert_called_once_with('blah')

    def test_cql_array(self):
        expected = [
            {"cols": [{"name": "foo", "timestamp": None, 'ttl': None, "value": "{P}"}],
             "key": "blah"}]

        mockrow = [ttypes.CqlRow(key='blah', columns=[ttypes.Column(name='foo', value='{P}')])]
        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.ROWS, rows=mockrow)
        client = CQLClient(self.endpoint, 'blah')

        d = client.execute("SELECT :sel FROM test_blah", {"sel": "blah"})
        self.assertEqual(self.assertFired(d), expected)
        self.client_proto.execute_cql_query.assert_called_once_with("SELECT 'blah' FROM test_blah", 2)
        self.client_proto.set_keyspace.assert_called_once_with('blah')
        self.client_proto.describe_keyspace.assert_called_once_with('blah')

    def test_cql_array_deserial(self):
        expected = [
            {"cols": [{"name": "fff", "timestamp": None, 'ttl': None, "value": 1222}],
             "key": "blah"}]

        mockrow = [ttypes.CqlRow(key='blah', columns=[ttypes.Column(name='fff', value='\x04\xc6')])]
        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.ROWS, rows=mockrow)
        client = CQLClient(self.endpoint, 'blah')

        d = client.execute("SELECT * FROM :tablename;", {"tablename": "blah"})
        self.assertEqual(self.assertFired(d), expected)
        self.client_proto.execute_cql_query.assert_called_once_with("SELECT * FROM 'blah';", 2)
        self.client_proto.set_keyspace.assert_called_once_with('blah')
        self.client_proto.describe_keyspace.assert_called_once_with('blah')

    def test_cql_insert(self):
        expected = None

        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.VOID)
        client = CQLClient(self.endpoint, 'blah')

        d = client.execute("UPDATE blah SET 'key'='frr', 'fff'=1222 WHERE KEY='frr'", {})
        self.assertEqual(self.assertFired(d), expected)
        self.client_proto.execute_cql_query.assert_called_once_with(
            "UPDATE blah SET 'key'='frr', 'fff'=1222 WHERE KEY='frr'",
            2)
        self.client_proto.set_keyspace.assert_called_once_with('blah')
        self.client_proto.describe_keyspace.assert_called_once_with('blah')

    def test_cql_insert_vars(self):
        expected = None

        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.VOID)
        client = CQLClient(self.endpoint, 'blah')

        d = client.execute("UPDATE blah SET 'key'='frr', 'fff'=:val WHERE KEY='frr'", {"val": 1234})
        self.assertEqual(self.assertFired(d), expected)
        self.client_proto.execute_cql_query.assert_called_once_with(
            "UPDATE blah SET 'key'='frr', 'fff'=1234 WHERE KEY='frr'",
            2)
        self.client_proto.set_keyspace.assert_called_once_with('blah')
        self.client_proto.describe_keyspace.assert_called_once_with('blah')

    def test_cql_sequence(self):
        expected = [
            {"cols": [{"name": "foo", "timestamp": None, 'ttl': None, "value": "{P}"}],
             "key": "blah"}]

        mockrow = [ttypes.CqlRow(key='blah', columns=[ttypes.Column(name='foo', value='{P}')])]
        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.ROWS, rows=mockrow)
        client = CQLClient(self.endpoint, 'blah')

        def _cqlProc(r):
            return client.execute("SELECT :sel FROM test_blah", {"sel": "blah"})

        d = client.execute("SELECT :sel FROM test_blah", {"sel": "ffh"})
        d.addCallback(_cqlProc)
        self.assertEqual(self.assertFired(d), expected)
        self.client_proto.execute_cql_query.assert_any_call("SELECT 'blah' FROM test_blah", 2)
        self.client_proto.execute_cql_query.assert_any_call("SELECT 'ffh' FROM test_blah", 2)
        self.client_proto.set_keyspace.assert_called_once_with('blah')
        self.client_proto.describe_keyspace.assert_called_once_with('blah')


# class FaultTestCase(BaseTestCase):
#     def setUp(self):
#         self.client = CqlClient(TCP4ClientEndpoint(reactor, '127.0.0.1', 9160), 'blah')

#     def test_vers(self):
#         d = self.client.describe_version()
#         def printR(r):
#             print r
#         d.addCallback(printR)
#         return d

#     def test_cql(self):
#         d = self.client.execute("SELECT * FROM blah;", {})
#         def printQ(r):
#             print r
#         d.addCallback(printQ)
#         return d
