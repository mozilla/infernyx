import unittest
from infernyx.rule_helpers import parse_urls, parse_distinct


class TestPreprocess(unittest.TestCase):
    def setUp(self):
        pass

    def testParseUrls(self):
        input_parts = [
            {"tiles": [{"url": "www.google.com", "score": 1}, {"url": "www.facebook.com"}, {"url": "espn.go.com"}, {"id": 610}], "view": 6},
            {"tiles": [{"score": 1}, {}, {"url": "espn.go.com"}, {"id": 610}], "view": 6},
            {"tiles": [{"score": 1}, {}, {}, {"id": 610}], "view": 6},
            # test removal of duplicate tiles
            {"tiles": [{"url": "www.google.com", "score": 1},
                       {"url": "espn.go.com"},
                       {"url": "www.facebook.com"},
                       {"url": "www.facebook.com"},
                       {"url": "espn.go.com"},
                       {"url": "www.google.com"},
                       {"id": 610}],
             "view": 6
            },
        ]
        expected_pairs = [
            {("espn.go.com", "www.facebook.com"),
             ("espn.go.com", "espn.go.com"),
             ("espn.go.com", "www.google.com"),
             ("www.google.com", "www.google.com"),
             ("www.facebook.com", "www.google.com"),
             ("www.facebook.com", "www.facebook.com")
            },
            {("espn.go.com", "espn.go.com")},
            {},
            # test removal of duplicate tiles
            {("espn.go.com", "www.facebook.com"),
             ("espn.go.com", "espn.go.com"),
             ("espn.go.com", "www.google.com"),
             ("www.google.com", "www.google.com"),
             ("www.facebook.com", "www.google.com"),
             ("www.facebook.com", "www.facebook.com")
            },
        ]
        for parts, expected in zip(input_parts, expected_pairs):
            for item in parse_urls(parts, None):
                # print item
                tup = (item['url_a'], item['url_b'])
                self.assertIn(tup, expected)
                expected.discard(tup)
                self.assertEqual(item["count"], 1)
            # assert that expected is empty - all pairs should have been seen and discarded
            self.assertEqual(len(expected), 0)

    def testParseDistinct(self):
        input_parts = [
            {"tiles": [{"url": "www.google.com", "score": 1}, {"url": "www.facebook.com"}, {"url": "espn.go.com"}, {"id": 610}], "view": 6},
            {"tiles": [{"score": 1}, {}, {"url": "espn.go.com"}, {"id": 610}], "view": 6},
            {"tiles": [{"score": 1}, {}, {}, {"id": 610}], "view": 6},
            # test removal of duplicate tiles
            {"tiles": [{"url": "www.google.com", "score": 1},
                       {"url": "espn.go.com"},
                       {"url": "www.facebook.com"},
                       {"url": "www.facebook.com"},
                       {"url": "espn.go.com"},
                       {"url": "www.google.com"},
                       {"id": 610}],
             "view": 6
            },
        ]
        expected_counts = [3, 1, 0, 3]
        for parts, expected in zip(input_parts, expected_counts):
            for item in parse_distinct(parts, None):
                # print item
                self.assertEqual(item["distinct_urls"], expected)
                self.assertEqual(item["count"], 1)

