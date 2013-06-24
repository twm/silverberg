# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Tests for marshal.py
"""

import iso8601
import struct

from twisted.trial.unittest import TestCase

from silverberg.marshal import marshal, unmarshal_timestamp


class MarshallingUnmarshallingTests(TestCase):
    """
    Test marshalling and unmarshalling of different types
    """

    def test_marshal_datetime(self):
        """
        Datetime objects are marshalled in UTC time and unmarshalled as UTC
        """
        datetimes = [
                     # Naive datetime is considered as UTC. Same is returned
                     ('2012-10-20T04:15:34.345+00:00', '2012-10-20T04:15:34.345'),
                     # TZ-aware datetime is stored and given as UTC (this tests for +4)
                     ('2012-10-20T04:15:34.654+04:00', '2012-10-20T00:15:34.654'),
                     # TZ-aware datetime is stored and given as UTC (this tests for -4)
                     ('2012-10-20T05:15:34.985-04:00', '2012-10-20T09:15:34.985')]
        for timestr, expected_utc_str in datetimes:
            dt = iso8601.parse_date(timestr)
            marshalled = marshal(dt)
            epoch = int(marshalled)
            epoch_bytes = struct.pack('>q', epoch)
            expected_utc = iso8601.parse_date(expected_utc_str).replace(tzinfo=None)
            self.assertEqual(unmarshal_timestamp(epoch_bytes), expected_utc)
