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

Example locking table creation:

CREATE TABLE locks ("lockId" ascii, "uuid" ascii, PRIMARY KEY("lockId","uuid"));

"""

import uuid
from silverberg.client import ConsistencyLevel
from twisted.internet.defer import (succeed, fail)

_lock_insert_query = 'INSERT INTO {cf} ("lockId","uuid") VALUES (:lockId,:uuid);'
_lock_read_query = 'SELECT COUNT(*) FROM {cf} WHERE "lockId"=:lockId;'
_lock_delete_query = 'DELETE FROM {cf} WHERE "lockId"=:lockId AND "uuid"=:uuid;'


class UnableToAcquireLockError(Exception):
    def __init__(self, lock_table, lock_id):
        super(UnableToAcquireLockError, self).__init__(
            "Unable to acquire lock {id} on {table}").format(id=lock_id,
                                                             table=lock_table)


class BasicLock:
    def __init__(self, client, lock_table, lock_id):
        self.client = client
        self.lock_table = lock_table
        self.lock_id = lock_id
        self._lock_uuid = uuid.uuid4()

    def release(self):
        d = self.client.execute(_lock_delete_query.format(cf=self.lock_table),
                                {'lockId': self.lock_id, 'uuid': self._lock_uuid},
                                ConsistencyLevel.QUORUM)
        return d

    def acquire(self):
        def _readBack(ignored):
            return self.client.execute(_lock_read_query.format(cf=self.lock_table),
                                       {'lockId': self.lock_id}, ConsistencyLevel.QUORUM)

        def _checkReadBack(count):
            # Parse response!
            if (count == 1):
                return succeed(None)
            else:
                self.release().addCallback(lambda _:
                                           fail(UnableToAcquireLockError(self.lock_table,
                                                                         self.lock_id)))

        d = self.client.execute(_lock_insert_query.format(cf=self.lock_table),
                                {'lockId': self.lock_id, 'uuid': self._lock_uuid},
                                ConsistencyLevel.QUORUM)
        d.addCallback(_readBack)
        d.addCallback(_checkReadBack)
        return d
