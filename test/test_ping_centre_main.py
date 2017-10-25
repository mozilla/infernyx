import unittest
import random
import uuid
import datetime

from infernyx.rule_helpers import ping_centre_main_filter, clean_ping_centre_main


UUID = [str(uuid.uuid4()) for _i in range(10)]
VERSION = ["1.0.0", "1.0.1", "1.0.2", "1.0.3"]
SHIELD_ID = ["SHIELD01", "SHIELD02"]
UA = ["python-requests/2.9.1"]
EVENT = ["event1", "event2"]
RELEASE_CHANNEL = ["nightly", "default", "beta"]
IP = ["15.211.153.0", "10.192.171.13"]


def generate_ping_centre_main_payload():
    payload = {
        "topic": "main",
        "shield_id": random.choice(SHIELD_ID),
        "client_id": random.choice(UUID),
        "release_channel": random.choice(RELEASE_CHANNEL),
        "event": random.choice(EVENT),
    }
    return payload


def attach_extra_info(ping):
    ping["ip"] = random.choice(IP)
    ping["ua"] = random.choice(UA)
    now = datetime.datetime.utcnow()
    ping["date"] = now.strftime("%Y-%m-%d")
    ping["timestamp"] = int(now.strftime("%s")) * 1000
    ping["locale"] = "en-US"
    return ping


class TestPingCentreMain(unittest.TestCase):
    def setUp(self):
        self.params = {}
        super(TestPingCentreMain, self).setUp()

        self.MAIN_PINGS = [attach_extra_info(generate_ping_centre_main_payload()) for i in range(5)]
        self.FIXTURE = self.MAIN_PINGS

    def test_filters(self):
        n_main_logs = 0

        for line in self.FIXTURE:
            for _ in ping_centre_main_filter(line, self.params):
                n_main_logs += 1

        self.assertEqual(n_main_logs, 5)

    def test_clean_ping_centre_main(self):
        self.assertIsNotNone(clean_ping_centre_main(self.MAIN_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "release_channel", "event"]:
            line = self.MAIN_PINGS[0].copy()
            del line[field_name]
            ret = clean_ping_centre_main(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ['shield_id', 'value']:
            line = self.MAIN_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_ping_centre_main(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_ping_centre_main(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")
