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

from twisted.internet import defer, task

from silverberg.client import ConsistencyLevel
from silverberg.cassandra.ttypes import InvalidRequestException


class BusyLockError(Exception):
    def __init__(self, lock_table, lock_id):
        super(BusyLockError, self).__init__(
            "Unable to acquire lock {id} on {table}".format(id=lock_id,
                                                            table=lock_table))


class BasicLock(object):
    """A locking mechanism for Cassandra.

    Based on the lock implementation from Netflix's astyanax, the lock recipe
    is a write, read, write operation. A record is written to the specified
    Cassandra database table with a timeuuid, and then the table is read for
    the given lock, ordered by timeuuid. If the first row is not ours, the
    lock was not acquired, so a write to remove the lock is made.

    :param client: A Cassandra CQL client
    :type client: silverberg.client.CQLClient

    :param lock_table: A table/columnfamily table name for holding locks.
    :type lock_table: str

    :param lock_id: A unique identifier for the lock.
    :type lock_id: str

    :param ttl: A TTL for the lock.
    :type ttl: int

    :param max_retry: A number of times to retry acquisition of the lock.
    :type max_retry: int

    :param retry_wait: A number of seconds to wait before retrying acquisition.
    :type retry_wait: int

    :param reactor: A twisted clock.
    :type reactor: twisted.internet.interfaces.IReactorTime
    """

    def __init__(self, client, lock_table, lock_id, ttl=300, max_retry=0,
                 retry_wait=10, reactor=None):
        self._client = client
        self._lock_table = lock_table
        self._lock_id = lock_id
        self._claim_id = uuid.uuid1()
        self._ttl = ttl
        self._max_retry = max_retry
        self._retry_wait = retry_wait
        if reactor is None:
            from twisted.internet import reactor
        self._reactor = reactor

    def _read_lock(self, ignored):
        query = 'SELECT * FROM {cf} WHERE "lockId"=:lockId ORDER BY "claimId";'
        return self._client.execute(query.format(cf=self._lock_table),
                                    {'lockId': self._lock_id}, ConsistencyLevel.QUORUM)

    def _verify_lock(self, response):
        if response[0]['claimId'] == self._claim_id:
            return defer.succeed(True)
        else:
            return self.release().addCallback(lambda _: defer.fail(
                BusyLockError(self._lock_table, self._lock_id)))

    def _write_lock(self):
        query = 'INSERT INTO {cf} ("lockId","claimId") VALUES (:lockId,:claimId) USING TTL {ttl};'
        return self._client.execute(query.format(cf=self._lock_table, ttl=self._ttl),
                                    {'lockId': self._lock_id, 'claimId': self._claim_id},
                                    ConsistencyLevel.QUORUM)

    @staticmethod
    def ensure_schema(client, table_name):
        """
        Create the table/columnfamily if it doesn't already exist.

        :param client: A Cassandra CQL client
        :type client: silverberg.client.CQLClient

        :param lock_table: A table/columnfamily table name for holding locks.
        :type lock_table: str
        """
        query = ''.join([
            'CREATE TABLE {cf} ',
            '("lockId" ascii, "claimId" timeuuid, PRIMARY KEY("lockId", "claimId"));'])

        def errback(failure):
            failure.trap(InvalidRequestException)

        return client.execute(query.format(cf=table_name),
                              {}, ConsistencyLevel.QUORUM).addErrback(errback)

    @staticmethod
    def drop_schema(client, table_name):
        """
        Delete the table/columnfamily.

        :param client: A Cassandra CQL client
        :type client: silverberg.client.CQLClient

        :param lock_table: A table/columnfamily table name for holding locks.
        :type lock_table: str
        """
        query = 'DROP TABLE {cf}'
        return client.execute(query.format(cf=table_name),
                              {}, ConsistencyLevel.QUORUM)

    def release(self):
        """
        Release the lock.
        """
        query = 'DELETE FROM {cf} WHERE "lockId"=:lockId AND "claimId"=:claimId;'
        d = self._client.execute(query.format(cf=self._lock_table),
                                 {'lockId': self._lock_id, 'claimId': self._claim_id},
                                 ConsistencyLevel.QUORUM)
        return d

    def acquire(self):
        """
        Acquire the lock.

        If the lock can't be acquired immediately, retry a specified number of
        times, with a specified wait time.
        """
        retries = [0]

        def acquire_lock():
            d = self._write_lock()
            d.addCallback(self._read_lock)
            d.addCallback(self._verify_lock)
            d.addErrback(lock_not_acquired)
            return d

        def lock_not_acquired(failure):
            failure.trap(BusyLockError)
            retries[0] += 1
            if retries[0] <= self._max_retry:
                return task.deferLater(self._reactor, self._retry_wait, acquire_lock)
            else:
                return failure

        return acquire_lock()


def with_lock(lock, func, *args, **kwargs):
    """A 'context manager' for performing operations requiring a lock.

    :param lock: A BasicLock instance
    :type lock: silverberg.lock.BasicLock

    :param func: A callable to execute while the lock is held.
    :type func: function
    """
    d = lock.acquire()

    def release_lock(result):
        deferred = lock.release()
        return deferred.addCallback(lambda x: result)

    def lock_acquired(lock):
        return defer.maybeDeferred(func, *args, **kwargs).addBoth(release_lock)

    d.addCallback(lock_acquired)
    return d
