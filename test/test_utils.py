import unittest
from infernyx.utils import kv_reader

class TestUtils(unittest.TestCase):
    def setUp(self):
        pass

    def test_kv_reader(self):
        insie = [
            ([u'_default', u'2015-01-07', u'en-US', u'US', u'www.dishanywhere.com', u'www.kohls.com'], [4]),
        ]
        for a_dict in kv_reader(insie, keys=['a', 'b'], values=['x']):
            print "aDict = %s" % str(a_dict)
            self.assertDictContainsSubset(
                {'a': u'_default', 'b': u'2015-01-07', 'x': 4},
                a_dict
            )

    def test_kv_reader_with_keyset(self):
        input = [
            ([u'one', u'2015-01-07', u'en-US', u'US', u'www.dishanywhere.com', u'www.abc.com'], [3]),
            ([u'one', u'2015-01-07', u'en-US', u'US', u'www.dishanywhere.com', u'www.kohls.com'], [4]),
            ([u'two', u'2015-01-07', u'en-US', u'US', u'www.dishanywhere.com', u'www.abc.com'], [5]),
            ([u'two', u'2015-01-08', u'en-US', u'US', u'www.dishanywhere.com', u'www.kohls.com'], [6]),
            ([u'three', u'2015-01-07', u'en-US', u'US', u'www.dishanywhere.com', u'www.kohls.com'], [7]),
        ]
        a_dict = list(kv_reader(input, keyset='two', keys=['a', 'b'], values=['x']))
        print "aDict = %s" % str(a_dict)
        self.assertDictContainsSubset(
            {'a': u'two', 'b': u'2015-01-07', 'x': 5},
            a_dict[0]
        )
        self.assertDictContainsSubset(
            {'a': u'two', 'b': u'2015-01-08', 'x': 6},
            a_dict[1]
        )
        self.assertEqual(len(a_dict), 2)