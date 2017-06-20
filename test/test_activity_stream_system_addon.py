import unittest
import random
import uuid
import datetime
import sys
from itertools import combinations

from infernyx.rule_helpers import assa_session_filter, assa_event_filter,\
    clean_assa_session, clean_assa_event, assa_performance_filter, clean_assa_performance,\
    assa_masga_filter, clean_assa_masga


UUID = [str(uuid.uuid4()) for _i in range(10)]
PAGE = ["newtab", "about:home"]
SOURCE = ["recent_links", "recent_bookmarks", "frecent_links", "top_sites", "spotlight"]
IP = ["15.211.153.0", "10.192.171.13"]
VERSION = ["1.0.0", "1.0.1", "1.0.2", "1.0.3"]
UA = ["python-requests/2.9.1"]
EVENT = ["delete", "click", "search"]
LOAD_TRIGGER_TYPE = ["newtab", "restore", "refresh"]


def generate_session_payload():
    payload = {
        "action": "activity_stream_session",
        "client_id": random.choice(UUID),
        "addon_version": random.choice(VERSION),
        "page": random.choice(PAGE),
        "session_id": random.choice(UUID),
        "load_trigger_type": random.choice(LOAD_TRIGGER_TYPE),
        "session_duration": abs(long(random.gauss(2, 1) * 1000)),
        "load_trigger_ts": abs(random.gauss(1, 1) * 1000),
        "visibility_event_rcvd_ts": abs(random.gauss(200, 20)),
    }
    return payload


def generate_event_payload():
    payload = {
        "action": "activity_stream_user_event",
        "client_id": random.choice(UUID),
        "addon_version": random.choice(VERSION),
        "session_id": random.choice(UUID),
        "page": random.choice(PAGE),
        "event": random.choice(EVENT),
        "source": random.choice(SOURCE),
        "action_position": "1"
    }
    return payload


def generate_performance_payload():
    payload = {
        "action": "activity_stream_performance_event",
        "client_id": random.choice(UUID),
        "session_id": random.choice(UUID),
        "event_id": random.choice(UUID),
        "addon_version": random.choice(VERSION),
        "event": random.choice(EVENT),
        "page": random.choice(PAGE),
        "source": random.choice(SOURCE),
        "value": random.randint(0, 100),
    }
    return payload


def generate_masga_payload():
    payload = {
        "action": "activity_stream_undesired_event",
        "client_id": random.choice(UUID),
        "session_id": random.choice(UUID),
        "addon_version": random.choice(VERSION),
        "event": "SHOW_LOADER",
        "page": random.choice(PAGE),
        "source": random.choice(SOURCE),
        "value": random.randint(0, 100),
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


SESSION_PINGS = [attach_extra_info(generate_session_payload()) for i in range(5)]
EVENT_PINGS = [attach_extra_info(generate_event_payload()) for i in range(5)]
PERFORMANCE_PINGS = [attach_extra_info(generate_performance_payload()) for i in range(5)]
MASGA_PINGS = [attach_extra_info(generate_masga_payload()) for i in range(5)]

FIXTURE = SESSION_PINGS + EVENT_PINGS + PERFORMANCE_PINGS + MASGA_PINGS


class TestActivityStreamSystemAddon(unittest.TestCase):
    def setUp(self):
        self.params = {}
        super(TestActivityStreamSystemAddon, self).setUp()

    def test_filters(self):
        n_session_logs = 0
        n_event_logs = 0
        n_performance_logs = 0
        n_masga_logs = 0

        for line in FIXTURE:
            for _ in assa_session_filter(line, self.params):
                n_session_logs += 1

            for _ in assa_event_filter(line, self.params):
                n_event_logs += 1

            for _ in assa_performance_filter(line, self.params):
                n_performance_logs += 1

            for _ in assa_masga_filter(line, self.params):
                n_masga_logs += 1

        self.assertEqual(n_session_logs, 5)
        self.assertEqual(n_event_logs, 5)
        self.assertEqual(n_performance_logs, 5)
        self.assertEqual(n_masga_logs, 5)
        # test filters are mutually orthogonal
        n_total = 0
        for f1, f2 in combinations([assa_event_filter,
                                    assa_session_filter,
                                    assa_performance_filter,
                                    assa_masga_filter], 2):
            for line in FIXTURE:
                for item in f1(line, self.params):
                        for _ in f2(item, self.params):
                            n_total += 1
        self.assertEqual(n_total, 0)

    def test_clean_assa_session(self):
        self.assertIsNotNone(clean_assa_session(SESSION_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "addon_version", "page", "session_id",
                           "load_trigger_type", "session_duration"]:
            line = SESSION_PINGS[0].copy()
            del line[field_name]
            ret = clean_assa_session(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the numeric fields with invalid values
        for field_name in ["session_duration"]:
            line = SESSION_PINGS[0].copy()
            line[field_name] = 2 ** 32
            ret = clean_assa_session(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test those floating point fields with invalid values
        for f in ["load_trigger_ts", "visibility_event_rcvd_ts"]:
            line = SESSION_PINGS[0].copy()
            line[field_name] = -1000.0
            ret = clean_assa_session(line, self.params)
            self.assertRaises(StopIteration, ret.next)

            line[field_name] = sys.float_info.max
            ret = clean_assa_session(line, self.params)
            self.assertRaises(StopIteration, ret.next)

    def test_clean_assa_event(self):
        self.assertIsNotNone(clean_assa_event(EVENT_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "addon_version", "page", "event", "session_id"]:
            line = EVENT_PINGS[0].copy()
            del line[field_name]
            ret = clean_assa_event(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ['action_position', 'source']:
            line = EVENT_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_assa_event(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_assa_event(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

    def test_clean_assa_performance(self):
        self.assertIsNotNone(clean_assa_performance(PERFORMANCE_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "addon_version", "event"]:
            line = PERFORMANCE_PINGS[0].copy()
            del line[field_name]
            ret = clean_assa_performance(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ["page", "source", "event_id", "session_id"]:
            line = PERFORMANCE_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_assa_performance(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_assa_performance(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

        # test the filter on the numeric fields with invalid values
        for field_name in ["value"]:
            line = PERFORMANCE_PINGS[0].copy()
            line[field_name] = 2 ** 32
            parts = clean_assa_performance(line, self.params).next()
            self.assertEquals(parts[field_name], -1)

        # test the filter on the numeric fields with float
        for field_name in ["value"]:
            line = PERFORMANCE_PINGS[0].copy()
            line[field_name] = 100.4
            parts = clean_assa_performance(line, self.params).next()
            self.assertEquals(parts[field_name], 100)

    def test_clean_assa_masga(self):
        self.assertIsNotNone(clean_assa_masga(MASGA_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "addon_version", "event"]:
            line = MASGA_PINGS[0].copy()
            del line[field_name]
            ret = clean_assa_masga(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ["page", "source", "session_id"]:
            line = MASGA_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_assa_masga(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_assa_masga(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

        # test the filter on the numeric fields with invalid values
        for field_name in ["value"]:
            line = MASGA_PINGS[0].copy()
            line[field_name] = 2 ** 32
            ret = clean_assa_masga(line, self.params)
            self.assertEqual(ret.next()["value"], -1)

        # test the filter on the numeric fields with float
        for field_name in ["value"]:
            line = MASGA_PINGS[0].copy()
            line[field_name] = 100.4
            parts = clean_assa_masga(line, self.params).next()
            self.assertEquals(parts[field_name], 100)
