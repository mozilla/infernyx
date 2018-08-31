import unittest
import random
import uuid
import datetime
import sys
import json

from infernyx.rule_helpers import activity_stream_router_event_filter, clean_assa_router_event


UUID = [str(uuid.uuid4()) for _i in range(10)]
MESSAGE_ID = [str(uuid.uuid4()) for _i in range(10)]
ACTION = ["snippet_click_events", "onboarding_click_events"]
IP = ["15.211.153.0", "10.192.171.13"]
VERSION = ["1.0.0", "1.0.1", "1.0.2", "1.0.3"]
UA = ["python-requests/2.9.1"]
EVENT = ["delete", "click"]
RELEASE_CHANNEL = ["release", "beta", "nightly"]


def generate_event_payload():
    payload = {
        "action": "activity_stream_user_event",
        "impression_id": random.choice(UUID),
        "addon_version": random.choice(VERSION),
        "event": random.choice(EVENT),
        "action": random.choice(ACTION),
        "message_id": random.choice(MESSAGE_ID),
    }
    return payload


def attach_extra_info(ping):
    ping["topic"] = "activity-stream-router"
    ping["ip"] = random.choice(IP)
    ping["ua"] = random.choice(UA)
    now = datetime.datetime.utcnow()
    ping["date"] = now.strftime("%Y-%m-%d")
    ping["timestamp"] = int(now.strftime("%s")) * 1000
    ping["locale"] = "en-US"
    ping["release_channel"] = random.choice(RELEASE_CHANNEL)
    ping["shield_id"] = ""
    return ping


class TestActivityStreamRouter(unittest.TestCase):
    def setUp(self):
        self.params = {}
        super(TestActivityStreamRouter, self).setUp()

        self.EVENT_PINGS = [attach_extra_info(generate_event_payload()) for i in range(5)]
        self.FIXTURE = self.EVENT_PINGS

    def test_filters(self):
        n_event_logs = 0

        for line in self.FIXTURE:
            for _ in activity_stream_router_event_filter(line, self.params):
                n_event_logs += 1

        self.assertEqual(n_event_logs, 5)

    def test_clean_assa_router_event(self):
        self.assertIsNotNone(clean_assa_router_event(self.EVENT_PINGS[0].copy(), self.params).next())

        # test the filter on the required fields
        for field_name in ["impression_id", "addon_version", "action", "event", "message_id"]:
            line = self.EVENT_PINGS[0].copy()
            del line[field_name]
            ret = clean_assa_router_event(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ['release_channel', 'shield_id']:
            line = self.EVENT_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_assa_router_event(line, self.params).next())

            # test on "null" values on optional key
            line = self.EVENT_PINGS[0].copy()
            line[field_name] = None
            parts = clean_assa_router_event(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

        # test the "value" field
        for value in ['value', None, [1, 2, 3], {"foo": "bar"}]:
            line = self.EVENT_PINGS[0].copy()
            line["value"] = value

            if isinstance(value, (list, dict)):
                # a collection "value" type should be serialized by JSON
                val = line["value"]
                parts = clean_assa_router_event(line, self.params).next()
                self.assertEqual(parts["value"], json.dumps(val))
            elif value is None:
                # missing "value" will be replaced by an empty dict
                del line["value"]
                parts = clean_assa_router_event(line, self.params).next()
                self.assertEqual(parts["value"], json.dumps({}))
            else:
                # other "value" types should remain the same
                parts = clean_assa_router_event(line, self.params).next()
                self.assertEqual(parts["value"], value)

    def test_clean_assa_router_event_for_client_id(self):
        line = self.EVENT_PINGS[0].copy()
        line["client_id"] = UUID[0]
        parts = clean_assa_router_event(line, self.params).next()
        self.assertEqual(parts["impression_id"], line["client_id"])

        line["client_id"] = "n/a"
        parts = clean_assa_router_event(line, self.params).next()
        self.assertEqual(parts["impression_id"], line["impression_id"])
