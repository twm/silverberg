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
from datetime import datetime

from twisted.trial.unittest import TestCase

from silverberg.marshal import marshal, unmarshal_timestamp, unmarshal_int, \
    unmarshal_initializable_int, unmarshal_double, prepare


class StatementPreparation(TestCase):
    """
    Test preparint a query with optional parameters.
    """

    def test_prepare(self):
        result = prepare("string :with a colon :with", {'with': 'value'})
        self.assertEqual(result, "string 'value' a colon 'value'")

        result = prepare("string :with a colon :with", {})
        self.assertEqual(result, "string :with a colon :with")


class MarshallingUnmarshallingDatetime(TestCase):
    """
    Test marshalling and unmarshalling of different datetime
    """

    def marshal_unmarshal_datetime(self, to_marshal):
        """
        Marshal the datetime using ``marshal`` and unmarshal it using ``unmarshal``
        """
        marshalled_epoch = int(marshal(to_marshal))
        marshalled_epoch_bytes = struct.pack('>q', marshalled_epoch)
        return unmarshal_timestamp(marshalled_epoch_bytes)

    def test_datetime_marshal_naive(self):
        """
        Naive datetime is considered as UTC and marshalled as such. Same is returned while unmarshalling
        """
        to_marshal = datetime(2012, 10, 20, 4, 15, 24, 345000)
        self.assertEqual(self.marshal_unmarshal_datetime(to_marshal), to_marshal)

    def test_datetime_marshal_positive_tz(self):
        """
        positive TZ-aware datetime is marshalled in UTC. The UTC time is unmarshalled
        """
        to_marshal = iso8601.parse_date('2012-10-20T04:15:34.654+04:00')
        expected_utc = datetime(2012, 10, 20, 0, 15, 34, 654000)
        self.assertEqual(self.marshal_unmarshal_datetime(to_marshal), expected_utc)

    def test_datetime_marshal_negative_tz(self):
        """
        negative TZ-aware datetime is marshalled in UTC. The UTC time is unmarshalled
        """
        to_marshal = iso8601.parse_date('2012-10-20T04:15:34.154+04:00')
        expected_utc = datetime(2012, 10, 20, 0, 15, 34, 154000)
        self.assertEqual(self.marshal_unmarshal_datetime(to_marshal), expected_utc)


class MarshallingUnmarshallingInteger(TestCase):
    """
    Test marshalling and unmarshalling of integers
    """

    def test_unmarshal_int(self):
        marshaled = '\x00\x00\x00\x00\x00\x00\x00\x05'
        self.assertEqual(unmarshal_int(marshaled), 5)

    def test_unmarshal_initializable_int(self):
        marshaled = '\x00\x00\x00\x00\x00\x00\x00\x05'
        self.assertEqual(unmarshal_initializable_int(marshaled), 5)

        # could not be initialized, i.e., None
        self.assertEqual(unmarshal_initializable_int(None), None)


class MarshallingUnmarshallingDouble(TestCase):
    """
    Test marshalling and unmarshalling of doubles
    """
    def test_unmarshal_double(self):
        marshaled = '?\xc1\x99\x99\x99\x99\x99\x9a'
        self.assertEqual(unmarshal_double(marshaled), 0.1375)
