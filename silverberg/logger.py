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
        self.client = client
        self.log = log
        if clock:
            self.clock = clock
        else:
            from twisted.internet import reactor
            self.clock = reactor

    def execute(self, query, args, consistency):
        """
        See :py:func:`silverberg.client.CQLClient.execute`
        """
        start_seconds = self.clock.seconds()

        def record_time(result):
            seconds_taken = self.clock.seconds() - start_seconds
            log = self.log.bind(query=query, data=args, consistency=consistency,
                                seconds_taken=seconds_taken)
            if isinstance(result, Failure):
                log.msg('CQL query execution failed', failure=result)
            else:
                log.msg('CQL query executed successfully')
            return result

        return self.client.execute(query, args, consistency).addBoth(record_time)
