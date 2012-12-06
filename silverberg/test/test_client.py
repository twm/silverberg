from twisted.trial.unittest import TestCase
from twisted.internet import defer, reactor
import mock

from silverberg.client import CassandraClient

from twisted.internet.endpoints import TCP4ClientEndpoint

class MockClientTests(TestCase):
    def setUp(self):
        self.endpoint = mock.Mock()
        self.client_proto = mock.Mock()

        def _connect(factory):
            wrapper = mock.Mock()
            wrapper.wrapped = self.client_proto
            self.client_proto.client.describe_version.return_value = '1.2.3'
            return defer.succeed(wrapper)

        self.endpoint.connect.side_effect = _connect

    def assertFired(self, d):
        results = []
        d.addCallback(lambda r: results.append(r))
        self.assertEqual(len(results), 1)

        return results[0]

    def test_log(self):
        client = CassandraClient(self.endpoint,'')

        d = client.describe_version()
        self.assertEqual(self.assertFired(d), '1.2.3')
        self.assertEqual(self.client_proto.client.describe_version.call_count, 1)

    def test_log_already_connected(self):
        client = CassandraClient(self.endpoint,'')

        d = client.describe_version()

        self.assertEqual(self.assertFired(d), '1.2.3')
        self.assertEqual(self.client_proto.client.describe_version.call_count, 1)

        d = client.describe_version()
        self.assertEqual(self.assertFired(d), '1.2.3')
        self.assertEqual(self.client_proto.client.describe_version.call_count, 2)

    def test_log_while_connecting(self):
        connect_d = defer.Deferred()
        real_connect = self.endpoint.connect.side_effect

        def _delay_connect(factory):
            d = real_connect(factory)
            connect_d.addCallback(lambda _: d)

            return connect_d

        self.endpoint.connect.side_effect = _delay_connect
        client = CassandraClient(self.endpoint,'')

        d = client.describe_version()
        self.assertEqual(self.client_proto.client.describe_version.call_count, 0)

        d2 = client.describe_version()
        self.assertEqual(self.client_proto.client.describe_version.call_count, 0)

        connect_d.callback(None)

        self.assertEqual(self.assertFired(d), '1.2.3')
        self.assertEqual(self.assertFired(d2), '1.2.3')

        self.assertEqual(self.client_proto.client.describe_version.call_count, 2)

class FaultTestCase(TestCase):
    def setUp(self):
        self.client = CassandraClient(TCP4ClientEndpoint(reactor, '127.0.0.1', 9160),'')
        
    def test_vers(self):
        d = self.client.describe_version()
        def printR(r):
            print "F"
            print r
            print "Q"
        d.addCallback(printR)
        return d
        
        