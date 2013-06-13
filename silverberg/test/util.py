from twisted.trial.unittest import TestCase
from twisted.python import failure


class BaseTestCase(TestCase):
    def assertFired(self, deferred):
        result = []
        deferred.addBoth(result.append)
        if not result:
            self.fail(
                "Success result expected on %r, found no result instead" % (
                    deferred,))
        elif isinstance(result[0], failure.Failure):
            self.fail(
                "Success result expected on %r, "
                "found failure result instead:\n%s" % (
                    deferred, result[0].getTraceback()))
        else:
            return result[0]

    def assertFailed(self, d, *errorTypes):
        results = []
        d.addErrback(lambda r: results.append(r))
        self.assertEqual(len(results), 1)

        if errorTypes:
            self.assertNotEqual(results[0].check(*errorTypes), None)

        return results[0]
