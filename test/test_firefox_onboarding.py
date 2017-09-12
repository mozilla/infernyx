import unittest
import random
import uuid
import time
import datetime
import sys
from itertools import combinations

from infernyx.rule_helpers import firefox_onboarding_session_filter, firefox_onboarding_event_filter,\
    clean_firefox_onboarding_session, clean_firefox_onboarding_event

UUID = [str(uuid.uuid4()) for _i in range(10)]
PAGE = ["newtab", "about:home"]
TOUR_ID = ["private", "sync", "customize"]
IP = ["15.211.153.0", "10.192.171.13"]
VERSION = ["1.0.0", "1.0.1", "1.0.2", "1.0.3"]
UA = ["python-requests/2.9.1"]
EVENT = ["overlay-nav-click", "notification-click"]
CATEGORY = ["overlay_interaction", "notify_interaction"]
TOUR_SOURCE = ["icon", "watermark", "notification"]


def generate_session_payload():
    payload = {
        "topic": "firefox-onboarding-session",
        "client_id": random.choice(UUID),
        "addon_version": random.choice(VERSION),
        "page": random.choice(PAGE),
        "session_id": random.choice(UUID),
        "session_begin": time.time() * 1000,
        "session_end": time.time() * 1000 + abs(long(random.gauss(2, 1) * 1000)),
        "event": random.choice(EVENT),
        "impression": 1,
        "category": random.choice(CATEGORY),
        "tour_source": random.choice(TOUR_SOURCE)
    }
    return payload


def generate_event_payload():
    payload = {
        "topic": "firefox-onboarding-event",
        "client_id": random.choice(UUID),
        "addon_version": random.choice(VERSION),
        "session_id": random.choice(UUID),
        "page": random.choice(PAGE),
        "event": random.choice(EVENT),
        "tour_id": random.choice(TOUR_ID),
        "impression": 1,
        "category": random.choice(CATEGORY),
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


class TestFirefoxOnboarding(unittest.TestCase):
    def setUp(self):
        self.params = {}
        super(TestFirefoxOnboarding, self).setUp()

        self.SESSION_PINGS = [attach_extra_info(generate_session_payload()) for i in range(5)]
        self.EVENT_PINGS = [attach_extra_info(generate_event_payload()) for i in range(5)]
        self.FIXTURE = self.SESSION_PINGS + self.EVENT_PINGS

    def test_filters(self):
        n_session_logs = 0
        n_event_logs = 0

        for line in self.FIXTURE:
            for _ in firefox_onboarding_session_filter(line, self.params):
                n_session_logs += 1

            for _ in firefox_onboarding_event_filter(line, self.params):
                n_event_logs += 1

        self.assertEqual(n_session_logs, 5)
        self.assertEqual(n_event_logs, 5)

        # test filters are mutually orthogonal
        n_total = 0
        for f1, f2 in combinations([firefox_onboarding_session_filter,
                                    firefox_onboarding_event_filter], 2):
            for line in self.FIXTURE:
                for item in f1(line, self.params):
                        for _ in f2(item, self.params):
                            n_total += 1
        self.assertEqual(n_total, 0)

    def test_clean_firefox_onboarding_session(self):
        self.assertIsNotNone(clean_firefox_onboarding_session(self.SESSION_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "addon_version", "page", "event", "category", "tour_source",
                           "session_begin", "session_end"]:
            line = self.SESSION_PINGS[0].copy()
            del line[field_name]
            ret = clean_firefox_onboarding_session(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the bigint fields
        for field_name in ["session_begin", "session_end"]:
            line = self.SESSION_PINGS[0].copy()
            line[field_name] = sys.maxint + 1
            ret = clean_firefox_onboarding_session(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the session_begin is greater than session_end
        line = self.SESSION_PINGS[0].copy()
        line['session_begin'] = sys.maxint
        ret = clean_firefox_onboarding_session(line, self.params)
        self.assertRaises(StopIteration, ret.next)

        # test the filter on the numeric fields with invalid values
        for field_name in ["impression"]:
            line = self.SESSION_PINGS[0].copy()
            line[field_name] = 2 ** 32
            parts = clean_firefox_onboarding_session(line, self.params).next()
            self.assertEquals(parts[field_name], -1)

        # test the filter on the numeric fields with float
        for field_name in ["impression"]:
            line = self.SESSION_PINGS[0].copy()
            line[field_name] = 100.4
            parts = clean_firefox_onboarding_session(line, self.params).next()
            self.assertEquals(parts[field_name], 100)

        # test the filter on the optional fields
        for field_name in ['session_id']:
            line = self.EVENT_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_firefox_onboarding_event(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_firefox_onboarding_event(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

    def test_clean_firefox_onboarding_event(self):
        self.assertIsNotNone(clean_firefox_onboarding_event(self.EVENT_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "addon_version", "page", "event", "category"]:
            line = self.EVENT_PINGS[0].copy()
            del line[field_name]
            ret = clean_firefox_onboarding_event(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the numeric fields with invalid values
        for field_name in ["impression"]:
            line = self.EVENT_PINGS[0].copy()
            line[field_name] = 2 ** 32
            parts = clean_firefox_onboarding_event(line, self.params).next()
            self.assertEquals(parts[field_name], -1)

        # test the filter on the numeric fields with invalid values
        for field_name in ["impression"]:
            line = self.EVENT_PINGS[0].copy()
            line[field_name] = 100.4
            parts = clean_firefox_onboarding_event(line, self.params).next()
            self.assertEquals(parts[field_name], 100)

        # test the filter on the optional fields
        for field_name in ['tour_id', 'session_id']:
            line = self.EVENT_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_firefox_onboarding_event(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_firefox_onboarding_event(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")
