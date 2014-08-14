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
"""Test the client."""
import mock

from uuid import UUID

from twisted.internet import defer

from silverberg.client import CQLClient, ConsistencyLevel, TestingCQLClient

from silverberg.cassandra import ttypes, Cassandra

from silverberg.test.util import BaseTestCase


class MockClientTests(BaseTestCase):

    """Test the client."""

    def setUp(self):
        """Setup the mock objects for the tests."""
        self.endpoint = mock.Mock()
        self.client_proto = mock.Mock(Cassandra.Client)
        self.twisted_transport = mock.Mock()

        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.INT, num=1)

        self.client_proto.set_keyspace.return_value = defer.succeed(None)
        self.client_proto.login.return_value = defer.succeed(None)
        self.client_proto.describe_version.return_value = defer.succeed('1.2.3')

        def _execute_cql3_query(*args, **kwargs):
            return defer.succeed(self.mock_results)

        self.client_proto.execute_cql3_query.side_effect = _execute_cql3_query

        def _connect(factory):
            wrapper = mock.Mock()
            wrapper.transport = self.twisted_transport
            wrapper.wrapped.client = self.client_proto
            return defer.succeed(wrapper)

        self.endpoint.connect.side_effect = _connect

    def test_disconnect_on_cancel(self):
        """
        If allowed, cancellation of running query will also try to disconnect
        the TCP connection
        """
        self.client_proto.execute_cql3_query.side_effect = lambda *_: defer.Deferred()
        client = CQLClient(self.endpoint, 'abc', disconnect_on_cancel=True)
        client.disconnect = mock.Mock()

        d = client.execute('query', {}, ConsistencyLevel.ONE)

        self.assertNoResult(d)
        self.assertFalse(client.disconnect.called)

        d.cancel()
        self.failureResultOf(d, defer.CancelledError)
        client.disconnect.assert_called_one_with()

    def test_disconnect_on_cancel_returns_correct_value(self):
        """
        with disconnect_on_cancel=True, the value from execute_cql3_query is
        returned before cancellation
        """
        exec_d = defer.Deferred()
        self.client_proto.execute_cql3_query.side_effect = lambda *_: exec_d
        client = CQLClient(self.endpoint, 'abc', disconnect_on_cancel=True)
        client.disconnect = mock.Mock()

        d = client.execute('query', {}, ConsistencyLevel.ONE)

        self.assertNoResult(d)
        self.assertFalse(client.disconnect.called)
        exec_d.callback(self.mock_results)
        self.assertEqual(self.successResultOf(d), 1)
        self.assertFalse(client.disconnect.called)

    def test_no_disconnect_on_cancel(self):
        """
        If not given, cancellation of running query should not try to disconnect
        the TCP connection
        """
        self.client_proto.execute_cql3_query.side_effect = lambda *_: defer.Deferred()
        client = CQLClient(self.endpoint, 'abc', disconnect_on_cancel=False)
        client.disconnect = mock.Mock()

        d = client.execute('query', {}, ConsistencyLevel.ONE)

        self.assertNoResult(d)
        self.assertFalse(client.disconnect.called)

        d.cancel()
        self.failureResultOf(d, defer.CancelledError)
        self.assertFalse(client.disconnect.called)

    def test_disconnect(self):
        """
        When disconnect is called, the on demand thrift client is disconnected
        """
        client = CQLClient(self.endpoint, 'blah')
        self.assertFired(client.describe_version())
        client.disconnect()
        self.twisted_transport.loseConnection.assert_called_once_with()

    def test_login(self):
        """Test that login works as expected."""
        client = CQLClient(self.endpoint, 'blah', 'groucho', 'swordfish')

        d = client.describe_version()
        self.assertEqual(self.assertFired(d), '1.2.3')
        self.client_proto.describe_version.assert_called_once_with()

        self.client_proto.set_keyspace.assert_called_once_with('blah')

        creds = {'user': 'groucho', 'password': 'swordfish'}
        authreq = ttypes.AuthenticationRequest(creds)
        self.client_proto.login.assert_called_once_with(authreq)

    def test_bad_keyspace(self):
        """Ensure that a bad keyspace results in an errback."""
        self.client_proto.set_keyspace.return_value = defer.fail(ttypes.NotFoundException())
        client = CQLClient(self.endpoint, 'blah')

        d = client.describe_version()
        self.assertFailed(d, ttypes.NotFoundException)
        self.client_proto.set_keyspace.assert_called_once_with('blah')

    def test_describe_version(self):
        """Connect and check the version."""
        client = CQLClient(self.endpoint, 'blah')

        d = client.describe_version()
        self.assertEqual(self.assertFired(d), '1.2.3')
        self.assertEqual(self.client_proto.describe_version.call_count, 1)
        self.client_proto.set_keyspace.assert_called_once_with('blah')

    def test_unsupported_types_are_returned_as_bytes(self):
        """
        When a table includes a column of a type that is not explicitly
        supported we should return the raw bytes instead of attempting to
        unmarshal the data.
        """
        mock_rows = [ttypes.CqlRow(
            key='',
            columns=[
                ttypes.Column(
                    name='an_unknown_type',
                    value="\x00\x01")])]

        self.mock_results = ttypes.CqlResult(
            type=ttypes.CqlResultType.ROWS,
            rows=mock_rows,
            schema=ttypes.CqlMetadata(value_types={'an_unknown_type': 'an.unknown.type'}))

        client = CQLClient(self.endpoint, 'blah')
        d = client.execute("SELECT * FROM blah", {}, ConsistencyLevel.ONE)
        results = self.assertFired(d)

        self.assertEqual(results, [{'an_unknown_type': '\x00\x01'}])

    def test_cql_value(self):
        """
        Test that a CQL response that is an integer value is
        processed correctly (e.g. SELECT COUNT).

        """
        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.INT, num=1)
        client = CQLClient(self.endpoint, 'blah')

        d = client.execute("SELECT :sel FROM test_blah", {"sel": "blah"}, ConsistencyLevel.ONE)
        self.assertEqual(self.assertFired(d), 1)
        self.client_proto.execute_cql3_query.assert_called_once_with("SELECT 'blah' FROM test_blah", 2,
                                                                     ConsistencyLevel.ONE)
        self.client_proto.set_keyspace.assert_called_once_with('blah')

    def test_cql_array(self):
        """Test that a full CQL response (e.g. SELECT) works."""
        expected = [{"foo": "{P}"}]

        mockrow = [ttypes.CqlRow(key='blah', columns=[ttypes.Column(name='foo', value='{P}')])]
        self.mock_results = ttypes.CqlResult(
            type=ttypes.CqlResultType.ROWS,
            rows=mockrow,
            schema=ttypes.CqlMetadata(value_types={'foo': 'org.apache.cassandra.db.marshal.UTF8Type'}))
        client = CQLClient(self.endpoint, 'blah')

        d = client.execute("SELECT :sel FROM test_blah", {"sel": "blah"}, ConsistencyLevel.ONE)
        self.assertEqual(self.assertFired(d), expected)
        self.client_proto.execute_cql3_query.assert_called_once_with("SELECT 'blah' FROM test_blah", 2,
                                                                     ConsistencyLevel.ONE)
        self.client_proto.set_keyspace.assert_called_once_with('blah')

    def test_cql_array_deserial(self):
        """Make sure that values that need to be deserialized correctly are."""
        expected = [{"fff": 1222}]

        mockrow = [ttypes.CqlRow(key='blah', columns=[ttypes.Column(name='fff', value='\x04\xc6')])]
        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.ROWS,
                                             rows=mockrow,
                                             schema=ttypes.CqlMetadata(value_types={
                                                 'fff': 'org.apache.cassandra.db.marshal.IntegerType'
                                             }))
        client = CQLClient(self.endpoint, 'blah')

        d = client.execute("SELECT * FROM :tablename;", {"tablename": "blah"}, ConsistencyLevel.ONE)
        self.assertEqual(self.assertFired(d), expected)
        self.client_proto.execute_cql3_query.assert_called_once_with("SELECT * FROM 'blah';", 2,
                                                                     ConsistencyLevel.ONE)
        self.client_proto.set_keyspace.assert_called_once_with('blah')

    def test_cql_list_deserial(self):
        expected = [{'fff': ['ggg', 'hhh']}]

        mockrow = [ttypes.CqlRow(key='blah',
                                 columns=[ttypes.Column(name='fff',
                                                        value='\x00\x02\x00\x03ggg\x00\x03hhh')])]

        list_type = 'org.apache.cassandra.db.marshal.ListType'
        text_type = 'org.apache.cassandra.db.marshal.UTF8Type'
        text_list_type = list_type + '(' + text_type + ')'

        self.mock_results = ttypes.CqlResult(
            type=ttypes.CqlResultType.ROWS,
            rows=mockrow,
            schema=ttypes.CqlMetadata(value_types={'fff': text_list_type}))
        client = CQLClient(self.endpoint, 'blah')

        d = client.execute("SELECT * FROM :tablename;", {"tablename": "blah"}, ConsistencyLevel.ONE)
        self.assertEqual(self.assertFired(d), expected)
        self.client_proto.execute_cql3_query.assert_called_once_with("SELECT * FROM 'blah';", 2,
                                                                     ConsistencyLevel.ONE)
        self.client_proto.set_keyspace.assert_called_once_with('blah')

    def test_cql_None_not_deserialized(self):
        """
        If the value is None, it is not deserialized at all.
        """
        raw_rows = [ttypes.CqlRow(
            key='blah', columns=[ttypes.Column(name='fff', value=None)])]
        schema = ttypes.CqlMetadata(value_types={
            'fff': 'org.apache.cassandra.db.marshal.AlwaysFailType'})

        client = CQLClient(self.endpoint, 'blah')

        always_blow_up = mock.Mock(spec=[], side_effect=Exception)

        rows = client._unmarshal_result(schema, raw_rows, {
            'org.apache.cassandra.db.marshal.AlwaysFailType': always_blow_up
        })

        self.assertEqual(rows, [{'fff': None}])
        self.assertEqual(always_blow_up.call_count, 0)

    def test_cql_insert(self):
        """Test a mock CQL insert with a VOID response works."""
        expected = None

        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.VOID)
        client = CQLClient(self.endpoint, 'blah')

        d = client.execute("UPDATE blah SET 'key'='frr', 'fff'=1222 WHERE KEY='frr'", {},
                           ConsistencyLevel.ONE)
        self.assertEqual(self.assertFired(d), expected)
        self.client_proto.execute_cql3_query.assert_called_once_with(
            "UPDATE blah SET 'key'='frr', 'fff'=1222 WHERE KEY='frr'",
            2, ConsistencyLevel.ONE)
        self.client_proto.set_keyspace.assert_called_once_with('blah')

    def test_cql_insert_vars(self):
        """Test that a CQL insert that has variables works."""
        expected = None

        self.mock_results = ttypes.CqlResult(type=ttypes.CqlResultType.VOID)
        client = CQLClient(self.endpoint, 'blah')

        d = client.execute("UPDATE blah SET 'key'='frr', 'fff'=:val WHERE KEY='frr'", {"val": 1234},
                           ConsistencyLevel.ONE)
        self.assertEqual(self.assertFired(d), expected)
        self.client_proto.execute_cql3_query.assert_called_once_with(
            "UPDATE blah SET 'key'='frr', 'fff'=1234 WHERE KEY='frr'",
            2, ConsistencyLevel.ONE)
        self.client_proto.set_keyspace.assert_called_once_with('blah')

    def test_cql_sequence(self):
        """
        Test a sequence of operations results in only one handshake
        but two requests.

        """
        expected = [{"foo": "{P}"}]

        mockrow = [ttypes.CqlRow(key='blah', columns=[ttypes.Column(name='foo', value='{P}')])]
        self.mock_results = ttypes.CqlResult(
            type=ttypes.CqlResultType.ROWS, rows=mockrow,
            schema=ttypes.CqlMetadata(
                value_types={'foo': 'org.apache.cassandra.db.marshal.UTF8Type'}))
        client = CQLClient(self.endpoint, 'blah')

        def _cqlProc(r):
            return client.execute("SELECT :sel FROM test_blah", {"sel": "blah"},
                                  ConsistencyLevel.ONE)

        d = client.execute("SELECT :sel FROM test_blah", {"sel": "ffh"},
                           ConsistencyLevel.ONE)
        d.addCallback(_cqlProc)
        self.assertEqual(self.assertFired(d), expected)
        self.client_proto.execute_cql3_query.assert_any_call("SELECT 'blah' FROM test_blah", 2,
                                                             ConsistencyLevel.ONE)
        self.client_proto.execute_cql3_query.assert_any_call("SELECT 'ffh' FROM test_blah", 2,
                                                             ConsistencyLevel.ONE)
        self.client_proto.set_keyspace.assert_called_once_with('blah')

    def test_cql_result_metadata(self):
        """
        execute should use the metadata included with the CqlResult for
        deserializing values.
        """
        expected = [{"foo": UUID('114b8328-d1f1-11e2-8683-000c29bc9473')}]

        mockrow = [
            ttypes.CqlRow(
                key='blah',
                columns=[
                    ttypes.Column(
                        name='foo',
                        value='\x11K\x83(\xd1\xf1\x11\xe2\x86\x83\x00\x0c)\xbc\x94s')])]

        self.mock_results = ttypes.CqlResult(
            type=ttypes.CqlResultType.ROWS,
            rows=mockrow,
            schema=ttypes.CqlMetadata(value_types={
                'foo': 'org.apache.cassandra.db.marshal.TimeUUIDType'}))

        client = CQLClient(self.endpoint, 'blah')
        d = client.execute("SELECT * FROM blah;", {}, ConsistencyLevel.ONE)
        self.assertEqual(self.assertFired(d), expected)


class MockTestingClientTests(MockClientTests):
    """
    Test the conveniences provided by the testing client
    """

    def test_transport_exposed(self):
        """
        The transport exposed is the underlying twisted transport, if it exists
        """
        client = TestingCQLClient(self.endpoint, 'meh')
        self.assertEqual(client.transport, None)  # has not connected yet
        self.assertFired(client.describe_version())
        self.assertIs(client.transport, self.twisted_transport)

    def test_pause(self):
        """
        When pausing, stop reading and stop writing on the transport are called
        if the transport exists.
        """
        client = TestingCQLClient(self.endpoint, 'meh')
        client.pause()
        self.assertEqual(len(self.twisted_transport.stopReading.mock_calls), 0)
        self.assertEqual(len(self.twisted_transport.stopWriting.mock_calls), 0)

        self.assertFired(client.describe_version())
        client.pause()
        self.twisted_transport.stopReading.assert_called_one_with()
        self.twisted_transport.stopWriting.assert_called_one_with()

    def test_resume(self):
        """
        When resuming, start reading and start writing on the transport are
        called if the transport exists.
        """
        client = TestingCQLClient(self.endpoint, 'meh')
        client.pause()
        self.assertEqual(len(self.twisted_transport.startReading.mock_calls),
                         0)
        self.assertEqual(len(self.twisted_transport.startWriting.mock_calls),
                         0)

        self.assertFired(client.describe_version())
        client.pause()
        self.twisted_transport.startReading.assert_called_one_with()
        self.twisted_transport.startWriting.assert_called_one_with()

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
