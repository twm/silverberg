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

"""

Locking recipe for Cassandra

"""

import uuid

from twisted.internet import defer  # import fail, maybeDeferred, succeed

from silverberg.client import ConsistencyLevel
from silverberg.cassandra.ttypes import InvalidRequestException


class UnableToAcquireLockError(Exception):
    def __init__(self, lock_table, lock_id):
        super(UnableToAcquireLockError, self).__init__(
            "Unable to acquire lock {id} on {table}".format(id=lock_id,
                                                            table=lock_table))


class BasicLock(object):
    """A locking mechanism for Cassandra.

    Based on the lock implementation from Netflix's astyanax, the lock recipe
    is a write, read, write operation. A record is written to the specified
    Cassandra database table, then the table is read for the given lock. If the
    count of the result is not 1, the lock was not acquired, so a write to
    remove the lock is made. Otherwise, the lock is acquired.
    """

    def __init__(self, client, lock_table, lock_id, ttl=300):
        self._client = client
        self._lock_table = lock_table
        self._lock_id = lock_id
        self._lock_claimId = uuid.uuid1()
        self._ttl = ttl

    def _read_lock(self, ignored):
        query = 'SELECT * FROM {cf} WHERE "lockId"=:lockId ORDER BY "claimId";'
        return self._client.execute(query.format(cf=self._lock_table),
                                    {'lockId': self._lock_id}, ConsistencyLevel.QUORUM)

    def _verify_lock(self, response):
        if response[0]['claimId'] == self._lock_claimId:
            return defer.succeed(True)
        else:
            return self.release().addCallback(lambda _: defer.fail(
                UnableToAcquireLockError(self._lock_table, self._lock_id)))

    def _write_lock(self):
        query = 'INSERT INTO {cf} ("lockId","claimId") VALUES (:lockId,:claimId) USING TTL {ttl};'
        return self._client.execute(query.format(cf=self._lock_table, ttl=self._ttl),
                                    {'lockId': self._lock_id, 'claimId': self._lock_claimId},
                                    ConsistencyLevel.QUORUM)

    @staticmethod
    def ensure_schema(client, table_name):
        query = 'CREATE TABLE {cf} ("lockId" ascii, "claimId" timeuuid, PRIMARY KEY("lockId", "claimId"));'

        def _errback(failure):
            if failure.type is InvalidRequestException:
                # The table already exists
                return defer.succeed(None)
            else:
                return defer.fail(failure)

        return client.execute(query.format(cf=table_name),
                              {}, ConsistencyLevel.QUORUM).addErrback(_errback)

    @staticmethod
    def drop_schema(client, table_name):
        query = 'DROP TABLE {cf}'
        return client.execute(query.format(cf=table_name),
                              {}, ConsistencyLevel.QUORUM)

    def release(self):
        query = 'DELETE FROM {cf} WHERE "lockId"=:lockId AND "claimId"=:claimId;'
        d = self._client.execute(query.format(cf=self._lock_table),
                                 {'lockId': self._lock_id, 'claimId': self._lock_claimId},
                                 ConsistencyLevel.QUORUM)
        return d

    def acquire(self):
        deferred = defer.Deferred()

        def _fire_deferred(*args, **kwargs):
            deferred.callback(*args, **kwargs)

        d = self._write_lock()
        d.addCallback(self._read_lock)
        d.addCallback(self._verify_lock)
        d.addCallback(_fire_deferred)

        return deferred


def with_lock(client, lock_id, table, func, *args, **kwargs):
    """A context manager for performing operations requiring a lock."""
    lock = BasicLock(client, lock_id, table)

    d = lock.acquire()

    def release_lock(result):
        deferred = lock.release()
        return deferred.addCallback(lambda x: result)

    def lock_acquired(lock):
        return defer.maybeDeferred(func, *args, **kwargs).addBoth(release_lock)

    d.addCallback(lock_acquired)
    return d
