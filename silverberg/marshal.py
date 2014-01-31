
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

# Lifted from the Cassandra CQL driver

import re
import struct
from uuid import UUID
from datetime import datetime
import calendar

import cql

__all__ = ['prepare', 'marshal', 'unmarshal_noop', 'unmarshallers']

_param_re = re.compile(r"(?<!strategy_options)(:[a-zA-Z_][a-zA-Z0-9_]*)", re.M)

BYTES_TYPE = "org.apache.cassandra.db.marshal.BytesType"
ASCII_TYPE = "org.apache.cassandra.db.marshal.AsciiType"
UTF8_TYPE = "org.apache.cassandra.db.marshal.UTF8Type"
INTEGER_TYPE = "org.apache.cassandra.db.marshal.IntegerType"
INTEGER32_TYPE = "org.apache.cassandra.db.marshal.Int32Type"
LONG_TYPE = "org.apache.cassandra.db.marshal.LongType"
UUID_TYPE = "org.apache.cassandra.db.marshal.UUIDType"
LEXICAL_UUID_TYPE = "org.apache.cassandra.db.marshal.LexicalType"
TIME_UUID_TYPE = "org.apache.cassandra.db.marshal.TimeUUIDType"
TIMESTAMP_TYPE = "org.apache.cassandra.db.marshal.DateType"
COUNTER_TYPE = "org.apache.cassandra.db.marshal.CounterColumnType"
DOUBLE_TYPE = "org.apache.cassandra.db.marshal.DoubleType"

LIST_TYPE = "org.apache.cassandra.db.marshal.ListType"


def prepare(query, params):
    """
    For every match of the form ":param_name", call marshal
    on kwargs['param_name'] and replace that section of the query
    with the result
    """
    def repl(match):
        name = match.group(1)[1:]
        if name in params:
            return marshal(params[name])
        return ":%s" % name

    new, count = re.subn(_param_re, repl, query)

    if len(params) > count:
        raise cql.ProgrammingError("More keywords were provided "
                                   "than parameters")
    return new


def marshal(term):
    if isinstance(term, unicode):
        return "'%s'" % __escape_quotes(term.encode('utf8'))
    elif isinstance(term, str):
        return "'%s'" % __escape_quotes(term)
    elif isinstance(term, datetime):
        # If the datetime is naive, then it is considered UTC time and stored. If it is
        # timezone-aware, then its corresponding UTC time is stored
        return str(int(calendar.timegm(term.utctimetuple()) * 1000 + term.microsecond / 1e3))
    else:
        return str(term)


def unmarshal_noop(bytestr):
    return bytestr


def unmarshal_utf8(bytestr):
    return bytestr.decode("utf8")


def unmarshal_int(bytestr):
    return decode_bigint(bytestr)


def unmarshal_initializable_int(bytestr):
    """
    This is useful for counters, which may not be initialized (bytestring could be None).
    """
    if bytestr is None:
        return None
    return decode_bigint(bytestr)


_long_packer = struct.Struct('>q')

_double_packer = struct.Struct('>d')


def unmarshal_long(bytestr):
    return _long_packer.unpack(bytestr)[0]


def unmarshal_double(bytestr):
    return _double_packer.unpack(bytestr)[0]


def unmarshal_timestamp(bytestr):
    epoch = unmarshal_long(bytestr)
    return datetime.utcfromtimestamp(epoch / 1000.0)


def unmarshal_uuid(bytestr):
    return UUID(bytes=bytestr)


def unmarshal_list(objtype, bytesstr):
    result = []
    # First two bytes are an integer of list size
    numelements = unmarshal_int(bytesstr[:2])
    p = 2
    for n in range(numelements):
        # Two bytes of list element size, as an integer
        length = unmarshal_int(bytesstr[p:p + 2])
        p += 2
        # Next 'length' bytes need unmarshalling into the specific type
        value = unmarshallers[objtype](bytesstr[p:p + length])
        p += length
        result.append(value)

    return result


unmarshallers = {BYTES_TYPE:        unmarshal_noop,
                 ASCII_TYPE:        unmarshal_noop,
                 UTF8_TYPE:         unmarshal_utf8,
                 INTEGER_TYPE:      unmarshal_int,
                 INTEGER32_TYPE:    unmarshal_int,
                 LONG_TYPE:         unmarshal_long,
                 DOUBLE_TYPE:       unmarshal_double,
                 UUID_TYPE:         unmarshal_uuid,
                 LEXICAL_UUID_TYPE: unmarshal_uuid,
                 TIME_UUID_TYPE:    unmarshal_uuid,
                 TIMESTAMP_TYPE:    unmarshal_timestamp,
                 COUNTER_TYPE:      unmarshal_initializable_int,
                 LIST_TYPE:         unmarshal_list}


def decode_bigint(term):
    val = int(term.encode('hex'), 16)
    if (ord(term[0]) & 128) != 0:
        val = val - (1 << (len(term) * 8))
    return val


def __escape_quotes(term):
    assert isinstance(term, (str, unicode))
    return term.replace("\'", "''")
