
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

from twisted.trial.unittest import TestCase
import mock

from silverberg.logger import LoggingCQLClient

from twisted.internet import defer
from twisted.internet.task import Clock


class LoggingCQLClientTests(TestCase):
    """
    Tests for mod:`silverberg.logger`
    """

    def setUp(self):
        """
        Mock CQLClient and log instance
        """
        self.client = mock.Mock(spec=['execute', 'disconnect'])
        self.log = mock.Mock(spec=['msg'])
        self.clock = Clock()
        self.logclient = LoggingCQLClient(self.client, self.log, self.clock)

    def test_client_execute_success(self):
        """
        When client.execute succeeds, it time taken and parameters are recorded
        and result is returned
        """
        def _execute(*args):
            self.clock.advance(10)
            return defer.succeed('returnvalue')
        self.client.execute.side_effect = _execute
        result = self.logclient.execute('query', {'d1': 1, 'd2': 2}, 7)
        self.assertEqual(self.successResultOf(result), 'returnvalue')
        self.client.execute.assert_called_once_with('query', {'d1': 1, 'd2': 2}, 7)
        self.log.msg.assert_called_once_with('CQL query executed successfully', query='query',
                                             data={'d1': 1, 'd2': 2}, consistency=7, seconds_taken=10)

    def test_client_execute_failure(self):
        """
        When client.execute fails the time taken, args and failure are recorded. The failure
        is then returned
        """
        err = ValueError('v')

        def _execute(*args):
            self.clock.advance(10)
            return defer.fail(err)

        self.client.execute.side_effect = _execute
        result = self.logclient.execute('query', {'d1': 1, 'd2': 2}, 7)
        self.assertEqual(self.failureResultOf(result).value, err)
        self.client.execute.assert_called_once_with('query', {'d1': 1, 'd2': 2}, 7)
        self.log.msg.assert_called_once_with('CQL query execution failed', reason=mock.ANY,
                                             query='query', data={'d1': 1, 'd2': 2}, consistency=7,
                                             seconds_taken=10)
        _, kwargs = self.log.msg.call_args
        self.assertEqual(kwargs['reason'].value, err)

    def test_disconnect(self):
        """
        logclient.disconnect() calls internal client's disconnect()
        """
        self.client.disconnect.return_value = 'result'
        d = self.logclient.disconnect()
        self.client.disconnect.assert_called_once_with()
        self.assertEquals(d, 'result')
