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
"""Test the lock."""
import uuid

import mock
from twisted.internet import defer

from silverberg.client import CQLClient
from silverberg.lock import BasicLock, UnableToAcquireLockError
from silverberg.test.util import BaseTestCase


class BasicLockTest(BaseTestCase):
    """Test the lock."""

    def setUp(self):
        self.client = mock.create_autospec(CQLClient)
        self.table_name = 'lock'

        def _side_effect(*args, **kwargs):
            return defer.succeed(1)
        self.client.execute.side_effect = _side_effect

    def test__read_lock(self):
        lock_uuid = uuid.uuid4()
        expected = [
            'SELECT COUNT(*) FROM lock WHERE "lockId"=:lockId;',
            {'lockId': lock_uuid}, 2]

        lock = BasicLock(self.client, self.table_name, lock_uuid)
        d = lock._read_lock(None)

        self.assertEqual(self.assertFired(d), 1)
        self.client.execute.assert_called_once_with(*expected)

    def test__verify_lock(self):
        lock_uuid = uuid.uuid4()

        lock = BasicLock(self.client, self.table_name, lock_uuid)
        d = lock._verify_lock(1)

        result = self.assertFired(d)
        self.assertEqual(result, True)

    def test__verify_lock_release(self):
        lock_uuid = uuid.uuid4()

        def _side_effect(*args, **kwargs):
            return defer.fail(UnableToAcquireLockError(self.table_name, lock_uuid))
        self.client.execute.side_effect = _side_effect

        lock = BasicLock(self.client, self.table_name, lock_uuid)
        expected = [
            'DELETE FROM lock WHERE "lockId"=:lockId AND "claimId"=:claimId;',
            {'lockId': lock_uuid, 'claimId': lock._lock_claimId}, 2]

        d = lock._verify_lock(2)

        def _assert_failure(failure):
            self.client.execute.assert_called_once_with(*expected)
            self.assertEqual(type(failure.value), UnableToAcquireLockError)
        d.addErrback(_assert_failure)

    def test__write_lock(self):
        lock_uuid = uuid.uuid4()

        lock = BasicLock(self.client, self.table_name, lock_uuid)
        expected = [
            'INSERT INTO lock ("lockId","claimId") VALUES (:lockId,:claimId) USING TTL 300;',
            {'lockId': lock_uuid, 'claimId': lock._lock_claimId}, 2]

        d = lock._write_lock()

        self.assertEqual(self.assertFired(d), 1)
        self.client.execute.assert_called_once_with(*expected)

    def test_acquire(self):
        """Lock acquire should write and then read back its write."""
        lock_uuid = uuid.uuid4()

        lock = BasicLock(self.client, self.table_name, lock_uuid)

        d = lock.acquire()
        self.assertEqual(self.assertFired(d), True)

        expected = [
            mock.call('INSERT INTO lock ("lockId","claimId") VALUES (:lockId,:claimId) USING TTL 300;',
                      {'lockId': lock_uuid, 'claimId': lock._lock_claimId}, 2),
            mock.call('SELECT COUNT(*) FROM lock WHERE "lockId"=:lockId;',
                      {'lockId': lock_uuid}, 2)]

        self.assertEqual(self.client.execute.call_args_list, expected)
        #self.client.execute.assert_called_with(*expected)

    def test_release(self):
        lock_uuid = uuid.uuid4()

        lock = BasicLock(self.client, self.table_name, lock_uuid)
        expected = [
            'DELETE FROM lock WHERE "lockId"=:lockId AND "claimId"=:claimId;',
            {'lockId': lock_uuid, 'claimId': lock._lock_claimId}, 2]

        d = lock.release()

        self.assertFired(d)
        self.client.execute.assert_called_once_with(*expected)
