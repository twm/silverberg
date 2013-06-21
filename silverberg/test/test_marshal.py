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
        datetimes = [('2012-10-20T04:15:34.345+00:00', '2012-10-20T04:15:34.345'),  # no timezone
                     ('2012-10-20T04:15:34.654+04:00', '2012-10-20T00:15:34.654'), # +4 timezone
                     ('2012-10-20T05:15:34.985-04:00', '2012-10-20T09:15:34.985')]   # -4 timezone
        for timestr, expected_utc_str in datetimes:
            dt = iso8601.parse_date(timestr)
            marshalled = marshal(dt)
            epoch = int(marshalled)
            epoch_bytes = struct.pack('>q', epoch)
            expected_utc = iso8601.parse_date(expected_utc_str).replace(tzinfo=None)
            self.assertEqual(unmarshal_timestamp(epoch_bytes), expected_utc)

