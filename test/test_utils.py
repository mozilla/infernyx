import unittest
from infernyx.utils import kv_reader

class TestUtils(unittest.TestCase):
    def setUp(self):
        pass

    def test_kv_reader(self):
        input = [
            ([u'_default', u'2015-01-07', u'en-US', u'US', u'www.dishanywhere.com', u'www.kohls.com'], [4]),
        ]
        for aDict in kv_reader(input, keys=['a', 'b'], values=['x']):
            print "aDict = %s" % str(aDict)
            self.assertDictContainsSubset(
                {'a': u'_default', 'b': u'2015-01-07', 'x': 4},
                aDict
            )