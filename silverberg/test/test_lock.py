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
from twisted.internet import defer, task

from silverberg.client import CQLClient
from silverberg.lock import BasicLock, BusyLockError, with_lock
from silverberg.test.util import BaseTestCase
from silverberg.cassandra.ttypes import InvalidRequestException


class BasicLockTest(BaseTestCase):
    """Test the lock."""

    def setUp(self):
        self.client = mock.create_autospec(CQLClient)
        self.table_name = 'lock'

        def _side_effect(*args, **kwargs):
            return defer.succeed(1)
        self.client.execute.side_effect = _side_effect

    def test__read_lock(self):
        lock_uuid = uuid.uuid1()
        expected = [
            'SELECT * FROM lock WHERE "lockId"=:lockId ORDER BY "claimId";',
            {'lockId': lock_uuid}, 2]

        lock = BasicLock(self.client, self.table_name, lock_uuid)
        d = lock._read_lock(None)

        self.assertEqual(self.assertFired(d), 1)
        self.client.execute.assert_called_once_with(*expected)

    def test__verify_lock(self):
        lock_uuid = uuid.uuid1()

        lock = BasicLock(self.client, self.table_name, lock_uuid)
        d = lock._verify_lock([{'lockId': lock._lock_id, 'claimId': lock._claim_id}])

        result = self.assertFired(d)
        self.assertEqual(result, True)

    def test__verify_lock_release(self):
        lock_uuid = uuid.uuid1()

        def _side_effect(*args, **kwargs):
            return defer.succeed(None)
        self.client.execute.side_effect = _side_effect

        lock = BasicLock(self.client, self.table_name, lock_uuid)
        expected = [
            'DELETE FROM lock WHERE "lockId"=:lockId AND "claimId"=:claimId;',
            {'lockId': lock_uuid, 'claimId': lock._claim_id}, 2]

        d = lock._verify_lock([{'lockId': lock._lock_id, 'claimId': ''}])

        result = self.failureResultOf(d)
        self.assertTrue(result.check(BusyLockError))
        self.client.execute.assert_called_once_with(*expected)

    def test__write_lock(self):
        lock_uuid = uuid.uuid1()

        lock = BasicLock(self.client, self.table_name, lock_uuid, 1000)
        expected = [
            'INSERT INTO lock ("lockId","claimId") VALUES (:lockId,:claimId) USING TTL 1000;',
            {'lockId': lock_uuid, 'claimId': lock._claim_id}, 2]

        d = lock._write_lock()

        self.assertEqual(self.assertFired(d), 1)
        self.client.execute.assert_called_once_with(*expected)

    def test_ensure_schema(self):
        """BasicLock.ensure_schema creates the table/columnfamily."""
        expected = [
            'CREATE TABLE lock ("lockId" ascii, "claimId" timeuuid, PRIMARY KEY("lockId", "claimId"));',
            {}, 2]

        d = BasicLock.ensure_schema(self.client, 'lock')
        self.successResultOf(d)
        self.client.execute.assert_called_once_with(*expected)

    def test_ensure_schema_already_created(self):
        """
        BasicLock.ensure_schema doesn't explode on InvalidRequestException,
        meaning the table already exists.
        """
        def _side_effect(*args, **kwargs):
            return defer.fail(InvalidRequestException())
        self.client.execute.side_effect = _side_effect

        d = BasicLock.ensure_schema(self.client, 'lock')
        self.successResultOf(d)

    def test_drop_schema(self):
        """BasicLock.drop_schema deletes the table/columnfamily."""
        expected = [
            'DROP TABLE lock',
            {}, 2]

        d = BasicLock.drop_schema(self.client, 'lock')
        self.successResultOf(d)
        self.client.execute.assert_called_once_with(*expected)

    def test_acquire(self):
        """Lock acquire should write and then read back its write."""
        lock_uuid = uuid.uuid1()

        lock = BasicLock(self.client, self.table_name, lock_uuid)

        def _side_effect(*args, **kwargs):
            return defer.succeed([{'lockId': lock._lock_id,
                                   'claimId': lock._claim_id}])
        self.client.execute.side_effect = _side_effect

        d = lock.acquire()
        self.assertEqual(self.assertFired(d), True)

        expected = [
            mock.call('INSERT INTO lock ("lockId","claimId") VALUES (:lockId,:claimId) USING TTL 300;',
                      {'lockId': lock._lock_id, 'claimId': lock._claim_id}, 2),
            mock.call('SELECT * FROM lock WHERE "lockId"=:lockId ORDER BY "claimId";',
                      {'lockId': lock._lock_id}, 2)]

        self.assertEqual(self.client.execute.call_args_list, expected)

    def test_acquire_retry(self):
        """BasicLock.acquire will retry max_retry times."""
        lock_uuid = uuid.uuid1()

        clock = task.Clock()
        lock = BasicLock(self.client, self.table_name, lock_uuid, max_retry=1, reactor=clock)

        responses = [
            defer.fail(BusyLockError('', '')),
            defer.succeed(True)
        ]

        def _new_verify_lock(response):
            return responses.pop(0)
        lock._verify_lock = _new_verify_lock

        def _side_effect(*args, **kwargs):
            return defer.succeed([])
        self.client.execute.side_effect = _side_effect

        d = lock.acquire()

        clock.advance(20)
        self.assertEqual(self.assertFired(d), True)
        self.assertEqual(self.client.execute.call_count, 4)

    def test_acquire_retry_never_acquired(self):
        """BasicLock.acquire will retry max_retry times and then give up."""
        lock_uuid = uuid.uuid1()

        clock = task.Clock()
        lock = BasicLock(self.client, self.table_name, lock_uuid, max_retry=1, reactor=clock)

        responses = [
            defer.fail(BusyLockError('', '')),
            defer.fail(BusyLockError('', ''))
        ]

        def _new_verify_lock(response):
            return responses.pop(0)
        lock._verify_lock = _new_verify_lock

        def _side_effect(*args, **kwargs):
            return defer.succeed([])
        self.client.execute.side_effect = _side_effect

        d = lock.acquire()

        clock.advance(20)
        result = self.failureResultOf(d)
        self.assertTrue(result.check(BusyLockError))
        self.assertEqual(self.client.execute.call_count, 4)

    def test_acquire_retry_not_lock_error(self):
        """If an error occurs that is not lock related, it is propagated."""
        lock_uuid = uuid.uuid1()

        clock = task.Clock()
        lock = BasicLock(self.client, self.table_name, lock_uuid, max_retry=1, reactor=clock)

        responses = [
            defer.fail(NameError('Keep your foot off the blasted samoflange.')),
        ]

        def _new_verify_lock(response):
            return responses.pop(0)
        lock._verify_lock = _new_verify_lock

        def _side_effect(*args, **kwargs):
            return defer.succeed([])
        self.client.execute.side_effect = _side_effect

        d = lock.acquire()

        result = self.failureResultOf(d)
        self.assertTrue(result.check(NameError))

    def test_release(self):
        lock_uuid = uuid.uuid1()

        lock = BasicLock(self.client, self.table_name, lock_uuid)
        expected = [
            'DELETE FROM lock WHERE "lockId"=:lockId AND "claimId"=:claimId;',
            {'lockId': lock_uuid, 'claimId': lock._claim_id}, 2]

        d = lock.release()

        self.assertFired(d)
        self.client.execute.assert_called_once_with(*expected)


class WithLockTest(BaseTestCase):
    """Test the lock context manager."""

    def setUp(self):
        patcher = mock.patch('silverberg.lock.BasicLock',)
        self.addCleanup(patcher.stop)
        self.BasicLock = patcher.start()

        self.lock = mock.create_autospec(BasicLock)

        def _acquire(*args, **kwargs):
            return defer.succeed(None)
        self.lock.acquire.side_effect = _acquire

        def _release():
            return defer.succeed(None)
        self.lock.release.side_effect = _release

        self.BasicLock.return_value = self.lock

    def test_with_lock(self):
        """
        Acquire the lock, run the function, and release the lock.
        """
        lock_uuid = uuid.uuid1()

        def _func():
            return defer.succeed('Success')

        lock = self.BasicLock(None, 'lock', lock_uuid)
        d = with_lock(lock, _func)

        result = self.successResultOf(d)
        self.assertEqual(result, 'Success')
        self.lock.acquire.assert_called_once_with()
        self.lock.release.assert_called_once_with()

    def test_with_lock_not_acquired(self):
        """
        Raise an error if the lock isn't acquired.
        """
        def _side_effect(*args, **kwargs):
            return defer.fail(BusyLockError('', ''))
        self.lock.acquire.side_effect = _side_effect

        lock_uuid = uuid.uuid1()

        called = [False]

        def _func():
            called[0] = True
            return defer.succeed(None)

        lock = self.BasicLock(None, 'lock', lock_uuid)
        d = with_lock(lock, _func)

        result = self.failureResultOf(d)
        self.assertTrue(result.check(BusyLockError))
        self.assertFalse(called[0])
        self.assertEqual(self.lock.release.call_count, 0)

    def test_with_lock_func_errors(self):
        """
        If the func raises an error, the lock is released and the error passsed on.
        """
        lock_uuid = uuid.uuid1()

        def _func():
            return defer.fail(TypeError('The samoflange is broken.'))

        lock = self.BasicLock(None, 'lock', lock_uuid)
        d = with_lock(lock, _func)

        result = self.failureResultOf(d)
        self.assertTrue(result.check(TypeError))
        self.assertEqual(result.getErrorMessage(), 'The samoflange is broken.')

        self.lock.acquire.assert_called_once_with()
        self.lock.release.assert_called_once_with()
