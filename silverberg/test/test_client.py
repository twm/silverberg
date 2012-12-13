from twisted.trial.unittest import TestCase
from twisted.internet import defer, reactor
import mock

from silverberg.client import CassandraClient

from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.python.failure import Failure

from silverberg.cassandra import ttypes



class MockClientTests(TestCase):
    def setUp(self):
        self.endpoint = mock.Mock()
        self.client_proto = mock.Mock()

        ksDef = ttypes.KsDef(name='blah', cf_defs=[ttypes.CfDef(comment='', key_validation_class='org.apache.cassandra.db.marshal.AsciiType', min_compaction_threshold=4, key_cache_save_period_in_seconds=None, gc_grace_seconds=864000, default_validation_class='org.apache.cassandra.db.marshal.UTF8Type', max_compaction_threshold=32, read_repair_chance=0.1, compression_options={'sstable_compression': 'org.apache.cassandra.io.compress.SnappyCompressor'}, bloom_filter_fp_chance=None, id=1000, keyspace='blah', key_cache_size=None, replicate_on_write=True, subcomparator_type=None, merge_shards_chance=None, row_cache_provider=None, row_cache_save_period_in_seconds=None, column_type='Standard', memtable_throughput_in_mb=None, memtable_flush_after_mins=None, column_metadata=[], key_alias=None, dclocal_read_repair_chance=0.0, name='blah', compaction_strategy_options={}, row_cache_keys_to_save=None, compaction_strategy='org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', memtable_operations_in_millions=None, caching='KEYS_ONLY', comparator_type='org.apache.cassandra.db.marshal.UTF8Type', row_cache_size=None)], strategy_options={'replication_factor': '1'}, strategy_class='org.apache.cassandra.locator.SimpleStrategy', replication_factor=None, durable_writes=True)

        self.mock_results = ttypes.CqlResult(type= ttypes.CqlResultType.INT, num=1)
        self.client_proto.client.set_keyspace.return_value = defer.succeed(None)
        def _connect(factory):
            wrapper = mock.Mock()
            wrapper.wrapped = self.client_proto
            self.client_proto.client.describe_version.return_value = '1.2.3'
            self.client_proto.client.describe_keyspace.return_value = defer.succeed(ksDef);
            self.client_proto.client.execute_cql_query.return_value = self.mock_results
            return defer.succeed(wrapper)

        self.endpoint.connect.side_effect = _connect

    def assertFired(self, d):
        results = []
        d.addCallback(lambda r: results.append(r))
        self.assertEqual(len(results), 1)

        return results[0]

    def assertFailed(self, d):
        results = []
        d.addErrback(lambda r: results.append(r))
        self.assertEqual(len(results), 1)

    def test_login(self):
            
        client = CassandraClient(self.endpoint,'blah','groucho','swordfish')

        d = client.describe_version()
        self.assertEqual(self.assertFired(d), '1.2.3')
        self.assertEqual(self.client_proto.client.describe_version.call_count, 1)
        
        self.client_proto.client.set_keyspace.assert_called_once_with('blah')
        
        creds = {'user': 'groucho', 'password': 'swordfish'}
        authreq = ttypes.AuthenticationRequest(creds)
        self.client_proto.client.login.assert_called_once_with(authreq)

    def test_bad_keyspace(self):
        self.client_proto.client.set_keyspace.return_value = defer.fail(ttypes.NotFoundException())
        client = CassandraClient(self.endpoint,'blah')

        d = client.describe_version()
        self.assertFailed(d)
        self.client_proto.client.set_keyspace.assert_called_once_with('blah')

    def test_describe_version(self):
        client = CassandraClient(self.endpoint,'blah')

        d = client.describe_version()
        self.assertEqual(self.assertFired(d), '1.2.3')
        self.assertEqual(self.client_proto.client.describe_version.call_count, 1)
        self.client_proto.client.set_keyspace.assert_called_once_with('blah')
        self.client_proto.client.describe_keyspace.assert_called_once_with('blah')

    def test_log_already_connected(self):
        client = CassandraClient(self.endpoint,'blah')

        d = client.describe_version()

        self.assertEqual(self.assertFired(d), '1.2.3')
        self.assertEqual(self.client_proto.client.describe_version.call_count, 1)

        d = client.describe_version()
        self.assertEqual(self.assertFired(d), '1.2.3')
        self.assertEqual(self.client_proto.client.describe_version.call_count, 2)
        
        self.client_proto.client.set_keyspace.assert_called_once_with('blah')
        self.client_proto.client.describe_keyspace.assert_called_once_with('blah')

    def test_log_while_connecting(self):
        connect_d = defer.Deferred()
        real_connect = self.endpoint.connect.side_effect

        def _delay_connect(factory):
            d = real_connect(factory)
            connect_d.addCallback(lambda _: d)

            return connect_d

        self.endpoint.connect.side_effect = _delay_connect
        client = CassandraClient(self.endpoint,'blah')

        d = client.describe_version()
        self.assertEqual(self.client_proto.client.describe_version.call_count, 0)

        d2 = client.describe_version()
        self.assertEqual(self.client_proto.client.describe_version.call_count, 0)

        connect_d.callback(None)

        self.assertEqual(self.assertFired(d), '1.2.3')
        self.assertEqual(self.assertFired(d2), '1.2.3')

        self.assertEqual(self.client_proto.client.describe_version.call_count, 2)
        
        self.client_proto.client.set_keyspace.assert_called_once_with('blah')
        self.client_proto.client.describe_keyspace.assert_called_once_with('blah')
        
    def test_cql_value(self):
        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.INT, num=1)
        client = CassandraClient(self.endpoint,'blah')

        d = client.execute("SELECT :sel FROM test_blah", {"sel": "blah"})
        self.assertEqual(self.assertFired(d), 1)
        self.client_proto.client.execute_cql_query.assert_called_once_with("SELECT 'blah' FROM test_blah", 2)
        self.client_proto.client.set_keyspace.assert_called_once_with('blah')
        self.client_proto.client.describe_keyspace.assert_called_once_with('blah')

    def test_cql_array(self):
        mockrow=[ttypes.CqlRow(key='blah', columns=[ttypes.Column(name='foo', value='{P}')])]
        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.ROWS, rows = mockrow)
        client = CassandraClient(self.endpoint,'blah')

        d = client.execute("SELECT :sel FROM test_blah", {"sel": "blah"})
        self.assertEqual(self.assertFired(d), mockrow)
        self.client_proto.client.execute_cql_query.assert_called_once_with("SELECT 'blah' FROM test_blah", 2)
        self.client_proto.client.set_keyspace.assert_called_once_with('blah')
        self.client_proto.client.describe_keyspace.assert_called_once_with('blah')



class FaultTestCase(TestCase):
    def setUp(self):
        self.client = CassandraClient(TCP4ClientEndpoint(reactor, '127.0.0.1', 9160),'blah')
        
    """
    def test_vers(self):
        d = self.client.describe_version()
        def printR(r):
            print r
        d.addCallback(printR)
        return d
       """ 
    def test_cql(self):
        d = self.client.execute("SELECT * FROM blah;", {})
        def printQ(r):
            print r
        d.addCallback(printQ)
        
        
        