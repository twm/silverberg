from twisted.trial.unittest import TestCase

class BaseTestCase(TestCase):
    def assertFired(self, d):
        results = []
        d.addCallback(lambda r: results.append(r))
        self.assertEqual(len(results), 1)

        return results[0]

    def assertFailed(self, d, *errorTypes):
        results = []
        d.addErrback(lambda r: results.append(r))
        self.assertEqual(len(results), 1)

        if errorTypes:
            self.assertNotEqual(results[0].check(*errorTypes), None)

        return results[0]


