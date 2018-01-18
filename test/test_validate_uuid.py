import unittest
from infernyx.rule_helpers import validate_uuid4


class TestRuleHelperFunctions(unittest.TestCase):
    def setUp(self):
        pass

    def testInvalidateUuid4(self):
        parts = {
            "client_id": "6954f539-0c58-4b2f-afdf-d70c7d4d5e0c",
            "session_id": "{5cbc6ab2-b3b8-478a-8d79-159dcf3021a7}",
            "special_id": "n/a"
        }
        new_parts = validate_uuid4(parts, {}, ["client_id", "session_id"]).next()
        self.assertDictEqual(parts, new_parts)

        parts["client_id"] = "not_a_valid_uuid"
        self.assertRaises(StopIteration, validate_uuid4(parts, {}, ["client_id"]).next)
