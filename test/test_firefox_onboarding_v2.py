import unittest
import random
import uuid
import time
import datetime
import sys
from itertools import combinations

from infernyx.rule_helpers import firefox_onboarding_session_filter_v2, firefox_onboarding_event_filter_v2,\
    clean_firefox_onboarding_session_v2, clean_firefox_onboarding_event_v2

UUID = [str(uuid.uuid4()) for _i in range(10)]
PAGE = ["newtab", "about:home"]
TOUR_ID = ["private", "sync", "customize"]
IP = ["15.211.153.0", "10.192.171.13"]
VERSION = ["1.0.0", "1.0.1", "1.0.2", "1.0.3"]
UA = ["python-requests/2.9.1"]
TYPE = ["overlay-nav-click", "notification-click"]
CATEGORY = ["overlay_interaction", "notify_interaction"]
TOUR_SOURCE = ["icon", "watermark", "notification"]
TOUR_TYPE = ["new", "update"]
BUBBLE_STATE = ["bubble", "dot", "hide"]
NOTIFICATION_STATE = ["show", "hide", "finish"]
LOGO_STSTE = ["logo", "watermark"]


def generate_session_payload():
    payload = {
        "topic": "firefox-onboarding-session2",
        "client_id": random.choice(UUID),
        "addon_version": random.choice(VERSION),
        "page": random.choice(PAGE),
        "parent_session_id": random.choice(UUID),
        "root_session_id": random.choice(UUID),
        "session_id": random.choice(UUID),
        "session_begin": time.time() * 1000,
        "session_end": time.time() * 1000 + abs(long(random.gauss(2, 1) * 1000)),
        "type": random.choice(TYPE),
        "category": random.choice(CATEGORY),
        "tour_type": random.choice(TOUR_TYPE)
    }
    return payload


def generate_event_payload():
    payload = {
        "topic": "firefox-onboarding-event2",
        "client_id": random.choice(UUID),
        "addon_version": random.choice(VERSION),
        "bubble_state": random.choice(BUBBLE_STATE),
        "category": random.choice(CATEGORY),
        "page": random.choice(PAGE),
        "current_tour_id": random.choice(TOUR_ID),
        "logo_state": random.choice(LOGO_STSTE),
        "notification_impression": 1,
        "notification_state": random.choice(NOTIFICATION_STATE),
        "parent_session_id": random.choice(UUID),
        "root_session_id": random.choice(UUID),
        "target_tour_id": "some target tour id",
        "tour_type": random.choice(TOUR_TYPE),
        "type": random.choice(TYPE),
        "timestamp": time.time() * 1000,
        "width": 1000
    }
    return payload


def attach_extra_info(ping):
    ping["ip"] = random.choice(IP)
    ping["ua"] = random.choice(UA)
    now = datetime.datetime.utcnow()
    ping["date"] = now.strftime("%Y-%m-%d")
    ping["timestamp"] = int(now.strftime("%s")) * 1000
    ping["locale"] = "en-US"
    ping["release_channel"] = "release"
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
            for _ in firefox_onboarding_session_filter_v2(line, self.params):
                n_session_logs += 1

            for _ in firefox_onboarding_event_filter_v2(line, self.params):
                n_event_logs += 1

        self.assertEqual(n_session_logs, 5)
        self.assertEqual(n_event_logs, 5)

        # test filters are mutually orthogonal
        n_total = 0
        for f1, f2 in combinations([firefox_onboarding_session_filter_v2,
                                    firefox_onboarding_event_filter_v2], 2):
            for line in self.FIXTURE:
                for item in f1(line, self.params):
                        for _ in f2(item, self.params):
                            n_total += 1
        self.assertEqual(n_total, 0)

    def test_clean_firefox_onboarding_session_v2(self):
        self.assertIsNotNone(clean_firefox_onboarding_session_v2(self.SESSION_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "addon_version", "page", "category", "tour_type", "parent_session_id",
                           "root_session_id", "session_begin", "session_end", "type"]:
            line = self.SESSION_PINGS[0].copy()
            del line[field_name]
            ret = clean_firefox_onboarding_session_v2(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the bigint fields
        for field_name in ["session_begin", "session_end"]:
            line = self.SESSION_PINGS[0].copy()
            line[field_name] = sys.maxint + 1
            ret = clean_firefox_onboarding_session_v2(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the session_begin is greater than session_end
        line = self.SESSION_PINGS[0].copy()
        line['session_begin'] = sys.maxint
        ret = clean_firefox_onboarding_session_v2(line, self.params)
        self.assertRaises(StopIteration, ret.next)

        # test the session timestamps with other types
        for field_name in ["session_begin", "session_end"]:
            line = self.SESSION_PINGS[0].copy()
            line[field_name] = line[field_name] + .1
            parts = clean_firefox_onboarding_session_v2(line, self.params).next()
            self.assertTrue(isinstance(parts[field_name], int))

        # test the filter on the optional fields
        for field_name in ['session_id']:
            line = self.SESSION_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_firefox_onboarding_session_v2(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_firefox_onboarding_session_v2(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

    def test_clean_firefox_onboarding_event_v2(self):
        self.assertIsNotNone(clean_firefox_onboarding_event_v2(self.EVENT_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "addon_version", "page", "category", "tour_type", "parent_session_id",
                           "root_session_id", "current_tour_id", "logo_state", "notification_state", "type",
                           "target_tour_id"]:
            line = self.EVENT_PINGS[0].copy()
            del line[field_name]
            ret = clean_firefox_onboarding_event_v2(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the numeric fields with invalid values
        for field_name in ["notification_impression", "width"]:
            line = self.EVENT_PINGS[0].copy()
            line[field_name] = 2 ** 32
            parts = clean_firefox_onboarding_event_v2(line, self.params).next()
            self.assertEquals(parts[field_name], -1)

        # test the filter on the numeric fields with invalid values
        for field_name in ["notification_impression", "width"]:
            line = self.EVENT_PINGS[0].copy()
            line[field_name] = 100.4
            parts = clean_firefox_onboarding_event_v2(line, self.params).next()
            self.assertEquals(parts[field_name], 100)

        # test the filter on the bigint fields
        for field_name in ["timestamp"]:
            line = self.EVENT_PINGS[0].copy()
            line[field_name] = line[field_name] + .1
            parts = clean_firefox_onboarding_event_v2(line, self.params).next()
            self.assertTrue(isinstance(parts[field_name], int))

            line[field_name] = sys.maxint + 1
            ret = clean_firefox_onboarding_event_v2(line, self.params)
            self.assertRaises(StopIteration, ret.next)
