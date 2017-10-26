import unittest
from infernyx.rule_helpers import parse_time


class TestParseTime(unittest.TestCase):
    def test_parse_time_success(self):
        parts = {"timestamp": 1508945522043}
        parts = parse_time(parts, {}).next()
        self.assertGreaterEqual(parts.get("hour"), 0)
        self.assertGreaterEqual(parts.get("minute"), 0)

    def test_parse_time_failure(self):
        parts = {"timestamp": "not_a_timestamp"}
        ret = parse_time(parts, {})
        self.assertRaises(StopIteration, ret.next)
