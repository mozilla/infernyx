import unittest
from infernyx.rules import parse_urls


class TestPreprocess(unittest.TestCase):
    def setUp(self):
        pass

    def testParseUrls(self):
        input_parts = [
            {"tiles": [{"url": "www.google.com", "score": 1}, {"url": "www.facebook.com"}, {"url": "espn.go.com"}, {"id": 610}], "view": 6},
            {"tiles": [{"score": 1}, {}, {"url": "espn.go.com"}, {"id": 610}], "view": 6},
            {"tiles": [{"score": 1}, {}, {}, {"id": 610}], "view": 6},
        ]
        expected_pairs = [
            {("espn.go.com", "www.facebook.com"), ("espn.go.com", "espn.go.com"), ("espn.go.com", "www.google.com"), ("www.google.com", "www.google.com"), ("www.facebook.com", "www.google.com"), ("www.facebook.com", "www.facebook.com")},
            {("espn.go.com", "espn.go.com")},
            {("espn.go.com", "www.facebook.com"), ("espn.go.com", "www.google.com"), ("www.facebook.com", "www.google.com")},
        ]
        for parts, expected in zip(input_parts, expected_pairs):
            for item in parse_urls(parts, None):
                print item
                tup = (item['url_a'], item['url_b'])
                self.assertIn(tup, expected)
                expected.discard(tup)
                self.assertEqual(item["count"], 1)