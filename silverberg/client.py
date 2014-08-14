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

Client API library for the Silverberg Twisted Cassandra CQL interface.

"""

from silverberg.cassandra import Cassandra
from silverberg.cassandra import ttypes

from twisted.internet.defer import succeed, Deferred

from silverberg.marshal import prepare, unmarshallers

from silverberg.thrift_client import OnDemandThriftClient
from silverberg.cassandra.ttypes import ConsistencyLevel


class CQLClient(object):
    """
    Cassandra CQL Client object.

    Instantiate it and it will on-demand create a connection to the Cassandra
    cluster.

    :param cass_endpoint: A twisted Endpoint
    :type cass_endpoint: twisted.internet.interfaces.IStreamClientEndpoint

    :param keyspace: A keyspace to connect to
    :type keyspace: str.

    :param user: Username to connect with.
    :type user: str.

    :param password: Password to use in conjunction with Username.
    :type password: str.

    :param bool disconnect_on_cancel: Should the underlying TCP connection be disconnected
    when execute deferred is cancelled?

    Upon connecting, the client will authenticate (if paramaters are provided)
    and obtain the keyspace definition so that it can de-serialize properly.

    n.b. Cassandra presently doesn't have any real support for password
    authentication in the mainline as the simple access control options
    are disabled; you probably need to secure your Cassandra server using
    different methods and the password code isn't heavily tested.
    """

    def __init__(self, cass_endpoint, keyspace, user=None, password=None,
                 disconnect_on_cancel=False):
        self._client = OnDemandThriftClient(cass_endpoint, Cassandra.Client)

        self._keyspace = keyspace
        self._user = user
        self._password = password
        self._disconnect_on_cancel = disconnect_on_cancel

    def _set_keyspace(self, client):
        d = client.set_keyspace(self._keyspace)
        return d.addCallback(lambda _: client)

    def _login(self, client):
        creds = {'user': self._user, 'password': self._password}
        authreq = ttypes.AuthenticationRequest(creds)
        d = client.login(authreq)
        d.addCallback(lambda _: client)
        return d

    def _connection(self):
        def _handshake(client):
            d = succeed(client)
            if self._user and self._password:
                d.addCallback(self._login)
            d.addCallback(self._set_keyspace)
            return d

        ds = self._client.connection(_handshake)
        return ds

    def disconnect(self):
        """
        Disconnect from the cassandra cluster.  Cassandara and Silverberg do
        not require the connection to be closed before exiting.  However, this
        method may be useful if resources are constrained, or for testing
        purposes if using or injecting :class:`TestCQLClient: is impossible.

        :return: a :class:`Deferred` that fires with None when disconnected.
        """
        return self._client.disconnect()

    def describe_version(self):
        """
        Query the Cassandra server for the version.

        :returns: string -- the version tag
        """
        def _vers(client):
            return client.describe_version()

        d = self._connection()
        d.addCallback(_vers)
        return d

    def _unmarshal_result(self, schema, raw_rows, _unmarshallers):
        rows = []

        def _unmarshal_val(vtype, val):
            if val is None:
                return

            # Differentiate between regular and collection types
            # Collection types look like 'ListType(SomeCassandraType)',
            # so try split into two parts and check if we can marshal them
            types = str(vtype).replace(")", "").split("(")

            # Regular type
            if len(types) == 1 and vtype in _unmarshallers:
                return _unmarshallers[vtype](val)

            # Collection
            elif len(types) == 2 and types[0] in _unmarshallers and types[1] in _unmarshallers:
                return _unmarshallers[types[0]](types[1], val)

            # XXX: We do not currently implement the full range of types.
            # So we can not unmarshal all types in which case we should just
            # return the raw bytes.
            return val

        for raw_row in raw_rows:
            row = {}
            for raw_col in raw_row.columns:
                specific = schema.value_types[raw_col.name]
                row[raw_col.name] = _unmarshal_val(specific, raw_col.value)
            rows.append(row)
        return rows

    def execute(self, query, args, consistency):
        """
        Execute a CQL query against the server.

        :param query: The CQL query to execute
        :type query: str.

        :param args: The arguments to substitute
        :type args: dict.

        :param consistency: The consistency level
        :type consistency: ConsistencyLevel

        In order to avoid unpleasant issues of CQL injection
        (Hey, just because there's no SQL doesn't mean that Little
        Bobby Tables won't mess things up for you like in XKCD #327)
        you probably want to use argument substitution instead of
        concatting strings together to build a query.

        Thus, like the official CQL driver for non-Twisted python
        that comes with the Cassandra distro, we do variable substitution.

        Example::

            d = client.execute("UPDATE :table SET 'fff' = :val WHERE "
            "KEY = :key",{"val":1234, "key": "fff", "table": "blah"})

        :returns: A Deferred that fires with either None, an int, or an
                  iterable of `{'column': value, ...}` dictionaries, depending
                  on the CQL query.  e.g. a UPDATE would return None,
                  whereas a SELECT would return an int or some rows

        Example output::

            [{"fff": 1222}]
        """
        prep_query = prepare(query, args)

        def _execute(client):
            exec_d = client.execute_cql3_query(prep_query,
                                               ttypes.Compression.NONE, consistency)
            if self._disconnect_on_cancel:
                cancellable_d = Deferred(lambda d: self.disconnect())
                exec_d.chainDeferred(cancellable_d)
                return cancellable_d
            else:
                return exec_d

        def _proc_results(result):
            if result.type == ttypes.CqlResultType.ROWS:
                return self._unmarshal_result(result.schema, result.rows,
                                              unmarshallers)
            elif result.type == ttypes.CqlResultType.INT:
                return result.num
            else:
                return None

        d = self._connection()
        d.addCallback(_execute)
        d.addCallback(_proc_results)
        return d


class TestingCQLClient(CQLClient):
    """
    Cassandra CQL Client object to be used for testing purposes.  This client
    exposes the underlying Twisted transport and provides convenience functions
    so that it can be used in trial tests.

    Instantiate it and it will on-demand create a connection to the Cassandra
    cluster.

    :param cass_endpoint: A twisted Endpoint
    :type cass_endpoint: twisted.internet.interfaces.IStreamClientEndpoint

    :param keyspace: A keyspace to connect to
    :type keyspace: str.

    :param user: Username to connect with.
    :type user: str.

    :param password: Username to connect with.
    :type password: str.

    Upon connecting, the client will authenticate (if paramaters are provided)
    and obtain the keyspace definition so that it can de-serialize properly.

    n.b. Cassandra presently doesn't have any real support for password
    authentication in the mainline as the simple access control options
    are disabled; you probably need to secure your Cassandra server using
    different methods and the password code isn't heavily tested.
    """
    @property
    def transport(self):
        """
        Get the underlying Twisted transport.
        """
        return self._client._transport

    def pause(self):
        """
        Pause the client by removing the connection from the reactor.  This is
        useful in tests if, for instance, latency is a problem and you do not
        want to disconnect and reconnect between every test.  If you do not
        disconnect and reconnect, and you do not pause and resume, then if you
        use Twisted's testing framework (``trial``), tests will fail with a
        dirty reactor warning.
        """
        if self.transport:
            self.transport.stopReading()
            self.transport.stopWriting()

    def resume(self):
        """
        Resume the client by making sure the reactor is aware of the
        connection. This is useful in tests if, for instance, latency is a
        problem and you do not want to disconnect and reconnect between every
        test.  If you do not disconnect and reconnect, and you do not pause
        and resume, then if you use Twisted's testing framework (``trial``),
        tests will fail with a dirty reactor warning.
        """
        if self.transport:
            self.transport.startReading()
            self.transport.startWriting()


__all__ = ["CQLClient", "ConsistencyLevel", "TestingCQLClient"]
