import unittest
from infernyx.rule_helpers import parse_batch


class TestParseBatch(unittest.TestCase):
    def test_parse_batch_single(self):
        parts = {
            "client_id": "abc",
            "action": "click",
        }
        ret = parse_batch(parts, {}).next()
        self.assertEquals(parts, ret)

    def test_parse_batch_multiple(self):
        parts = {
            "client_id": "abc",
            "batch-mode": True,
            "payloads": [
                {"action": "click"},
                {"action": "delete"}
            ]
        }
        expected = [
            {"client_id": "abc", "action": "click"},
            {"client_id": "abc", "action": "delete"}
        ]
        ret = [payload for payload in parse_batch(parts, {})]
        self.assertEquals(len(ret), 2)
        for i, expected in enumerate(expected):
            self.assertEquals(ret[i], expected)

    def test_parse_batch_invalid(self):
        parts = {
            "client_id": "abc",
            "batch-mode": True,
            "payloads": "valid payloads"
        }
        ret = parse_batch(parts, {})
        self.assertRaises(StopIteration, ret.next)
