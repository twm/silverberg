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


class LoggableCQLClient(object):
    """
    A loggable CQL client. Every call will be timed and logged

    :param client: A `CQLClient` or `RoundRobinCassandraCluster` instance
    :param log: A bound logger that has .msg() method
    """

    def __init__(self, client, log):
        self.client = client
        self.log = log

    def execute(self, *args, **kwargs):
        """
        See :py:func:`silverberg.client.CQLClient.execute`
        """
        start_time = datetime.now()
        def record_time(result):
            time_taken = datetime.now() - start_time
            self.log.msg('CQL query executed', args=args, time_taken=time_taken)
            return result
        return self.client.execute(*args, **kwargs).addBoth(record_time)


