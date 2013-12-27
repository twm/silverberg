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


from twisted.python.failure import Failure


class LoggingCQLClient(object):
    """
    A logging CQL client. Every query will be timed and logged

    :param client: A `CQLClient` or `RoundRobinCassandraCluster` instance
    :param log: A bound logger that has .msg() method
    """

    def __init__(self, client, log, clock=None):
        self._client = client
        self._log = log
        if clock:
            self._clock = clock
        else:
            from twisted.internet import reactor
            self._clock = reactor

    def execute(self, query, args, consistency):
        """
        See :py:func:`silverberg.client.CQLClient.execute`
        """
        start_seconds = self._clock.seconds()

        def record_time(result):
            seconds_taken = self._clock.seconds() - start_seconds
            kwargs = dict(query=query, data=args, consistency=consistency,
                          seconds_taken=seconds_taken)
            if isinstance(result, Failure):
                self._log.msg('CQL query execution failed',
                              reason=result, **kwargs)
            else:
                self._log.msg('CQL query executed successfully', **kwargs)
            return result

        return self._client.execute(query, args, consistency).addBoth(record_time)

    def disconnect(self):
        """
        See :py:func:`silverberg.client.CQLClient.disconnect`
        """
        return self._client.disconnect()
