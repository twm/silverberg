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
from silverberg.client import ConsistencyLevel
from twisted.internet.defer import (succeed, fail)


class UnableToAcquireLockError(Exception):
    def __init__(self, lock_table, lock_id):
        super(UnableToAcquireLockError, self).__init__(
            "Unable to acquire lock {id} on {table}".format(id=lock_id,
                                                            table=lock_table))


class BasicLock(object):
    """A locking mechanism for Cassandra."""

    def __init__(self, client, lock_table, lock_id):
        self._client = client
        self._lock_table = lock_table
        self._lock_id = lock_id
        self._lock_claimId = uuid.uuid4()

    def _read_lock(self, ignored):
        query = 'SELECT COUNT(*) FROM {cf} WHERE "lockId"=:lockId;'
        return self._client.execute(query.format(cf=self._lock_table),
                                    {'lockId': self._lock_id}, ConsistencyLevel.QUORUM)

    def _verify_lock(self, count):
        # TODO: Parse response!
        if (count == 1):
            return succeed(True)
        else:
            return self.release().addCallback(lambda _:
                                              fail(UnableToAcquireLockError(self._lock_table,
                                                                            self._lock_id)))

    def _write_lock(self):
        query = 'INSERT INTO {cf} ("lockId","claimId") VALUES (:lockId,:claimId) USING TTL 300;'
        return self._client.execute(query.format(cf=self._lock_table),
                                    {'lockId': self._lock_id, 'claimId': self._lock_claimId},
                                    ConsistencyLevel.QUORUM)

    @staticmethod
    def ensure_schema(client, table_name):
        query = 'CREATE TABLE {cf} ("lockId" ascii, "claimId" ascii, PRIMARY KEY("lockId", "claimId"));'
        return client.execute(query.format(cf=table_name),
                              {}, ConsistencyLevel.QUORUM)

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
        d = self._write_lock()
        d.addCallback(self._read_lock)
        d.addCallback(self._verify_lock)
        return d
