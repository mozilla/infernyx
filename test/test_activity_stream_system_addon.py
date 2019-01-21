import unittest
import random
import uuid
import datetime
import sys
import json
from itertools import combinations

from infernyx.rule_helpers import assa_session_filter, assa_event_filter,\
    clean_assa_session, clean_assa_event, assa_performance_filter, clean_assa_performance,\
    assa_masga_filter, clean_assa_masga, assa_impression_filter, clean_assa_impression


UUID = [str(uuid.uuid4()) for _i in range(10)]
PAGE = ["newtab", "about:home"]
SOURCE = ["recent_links", "recent_bookmarks", "frecent_links", "top_sites", "spotlight"]
IP = ["15.211.153.0", "10.192.171.13"]
VERSION = ["1.0.0", "1.0.1", "1.0.2", "1.0.3"]
UA = ["python-requests/2.9.1"]
EVENT = ["delete", "click", "search"]
LOAD_TRIGGER_TYPE = ["newtab", "restore", "refresh"]
TILES = [[{"id": 10000}, {"id": 10001}, {"id": 10002}], [{"id": 10001, "pos": 2}]]
IMPRESSION_EVENTS = ["click", "block", "pocket"]
RELEASE_CHANNEL = ["release", "beta", "nightly"]


def generate_session_payload():
    payload = {
        "action": "activity_stream_session",
        "client_id": random.choice(UUID),
        "addon_version": random.choice(VERSION),
        "page": random.choice(PAGE),
        "session_id": random.choice(UUID),
        "session_duration": abs(long(random.gauss(2, 1) * 1000)),
        "region": "US",
        "profile_creation_date": 16587,
        "perf": {
            "load_trigger_type": random.choice(LOAD_TRIGGER_TYPE),
            "load_trigger_ts": abs(random.gauss(1, 1) * 1000),
            "visibility_event_rcvd_ts": abs(random.gauss(200, 20)),
            "topsites_first_painted_ts": abs(random.gauss(100, 10)),
            "is_preloaded": True,
            "is_prerendered": True,
            "topsites_data_late_by_ms": 50,
            "highlights_data_late_by_ms": 100,
            "topsites_icon_stats": {
                "screenshot_with_icon": 3,
                "screenshot": 2,
                "tippytop": 0,
                "rich_icon": 7,
                "no_image": 1
            },
            "topsites_pinned": 3,
            "topsites_search_shortcuts": 2
        }
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
        "action_position": "1",
        "profile_creation_date": 16587,
        "value": {
            "card_type": "bookmark",
            "icon_type": "screenshot"
        }
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


def generate_impression_payload():
    payload = {
        "action": "activity_stream_impression_stats",
        "client_id": random.choice(UUID),
        "session_id": random.choice(UUID),
        "addon_version": random.choice(VERSION),
        "page": random.choice(PAGE),
        "source": "TOP_STORIES",
        "region": "US",
        "tiles": random.choice(TILES)
    }
    if (len(payload["tiles"]) == 1):
        payload[random.choice(IMPRESSION_EVENTS)] = 0
    return payload


def attach_extra_info(ping):
    ping["ip"] = random.choice(IP)
    ping["ua"] = random.choice(UA)
    now = datetime.datetime.utcnow()
    ping["date"] = now.strftime("%Y-%m-%d")
    ping["timestamp"] = int(now.strftime("%s")) * 1000
    ping["locale"] = "en-US"
    ping["user_prefs"] = 7
    ping["release_channel"] = random.choice(RELEASE_CHANNEL)
    ping["shield_id"] = ""
    return ping


class TestActivityStreamSystemAddon(unittest.TestCase):
    def setUp(self):
        self.params = {}
        super(TestActivityStreamSystemAddon, self).setUp()

        self.SESSION_PINGS = [attach_extra_info(generate_session_payload()) for i in range(5)]
        self.EVENT_PINGS = [attach_extra_info(generate_event_payload()) for i in range(5)]
        self.PERFORMANCE_PINGS = [attach_extra_info(generate_performance_payload()) for i in range(5)]
        self.MASGA_PINGS = [attach_extra_info(generate_masga_payload()) for i in range(5)]
        self.IMPRESSION_PINGS = [attach_extra_info(generate_impression_payload()) for i in range(5)]
        self.FIXTURE = self.SESSION_PINGS + self.EVENT_PINGS + self.PERFORMANCE_PINGS + self.MASGA_PINGS +\
            self.IMPRESSION_PINGS

    def test_filters(self):
        n_session_logs = 0
        n_event_logs = 0
        n_performance_logs = 0
        n_masga_logs = 0
        n_impression_logs = 0

        for line in self.FIXTURE:
            for _ in assa_session_filter(line, self.params):
                n_session_logs += 1

            for _ in assa_event_filter(line, self.params):
                n_event_logs += 1

            for _ in assa_performance_filter(line, self.params):
                n_performance_logs += 1

            for _ in assa_masga_filter(line, self.params):
                n_masga_logs += 1

            for _ in assa_impression_filter(line, self.params):
                n_impression_logs += 1

        self.assertEqual(n_session_logs, 5)
        self.assertEqual(n_event_logs, 5)
        self.assertEqual(n_performance_logs, 5)
        self.assertEqual(n_masga_logs, 5)
        self.assertEqual(n_impression_logs, 5)

        # test filters are mutually orthogonal
        n_total = 0
        for f1, f2 in combinations([assa_event_filter,
                                    assa_session_filter,
                                    assa_performance_filter,
                                    assa_masga_filter,
                                    assa_impression_filter], 2):
            for line in self.FIXTURE:
                for item in f1(line, self.params):
                        for _ in f2(item, self.params):
                            n_total += 1
        self.assertEqual(n_total, 0)

    def test_clean_assa_session(self):
        self.assertIsNotNone(clean_assa_session(self.SESSION_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "addon_version", "page", "session_id",
                           "load_trigger_type"]:
            line = self.SESSION_PINGS[0].copy()
            del line[field_name]
            ret = clean_assa_session(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the invalid field on the required fields
        for field_name in ["load_trigger_type"]:
            line = self.SESSION_PINGS[0].copy()
            line[field_name] = "\\u0000\\u0000\\u0000\\u0000"
            parts = clean_assa_session(line, self.params).next()
            self.assertEqual(parts[field_name], "invalid")

        # test the filter on the numeric fields with invalid values
        int_fields = ["session_duration", "user_prefs", "topsites_data_late_by_ms",
                      "highlights_data_late_by_ms", "topsites_data_late_by_ms",
                      "screenshot_with_icon", "screenshot", "tippytop", "rich_icon",
                      "no_image", "topsites_pinned", "profile_creation_date",
                      "custom_screenshot", "topsites_search_shortcuts"]
        for field_name in int_fields:
            line = self.SESSION_PINGS[0].copy()
            line[field_name] = 2 ** 32
            parts = clean_assa_session(line, self.params).next()
            self.assertEquals(parts[field_name], -1)

        # test the filter on the numeric fields with float
        for field_name in int_fields:
            line = self.SESSION_PINGS[0].copy()
            line[field_name] = 100.4
            parts = clean_assa_session(line, self.params).next()
            self.assertEquals(parts[field_name], 100)

        # test those floating point fields with invalid values
        for field_name in ["load_trigger_ts", "visibility_event_rcvd_ts",
                           "topsites_first_painted_ts"]:
            line = self.SESSION_PINGS[0].copy()
            line[field_name] = -1000.0
            ret = clean_assa_session(line, self.params)
            self.assertRaises(StopIteration, ret.next)

            line[field_name] = sys.float_info.max
            ret = clean_assa_session(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ['release_channel', 'shield_id', 'region']:
            line = self.SESSION_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_assa_session(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_assa_session(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

        # test the filter on the optional boolean fields
        for field_name in ['is_preloaded', 'is_prerendered']:
            line = self.SESSION_PINGS[0].copy()
            del line[field_name]
            parts = clean_assa_session(line, self.params).next()
            self.assertFalse(parts[field_name])

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_assa_session(line, self.params).next()
            self.assertFalse(parts[field_name])

    def test_clean_assa_event(self):
        self.assertIsNotNone(clean_assa_event(self.EVENT_PINGS[0].copy(), self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "addon_version", "page", "event", "session_id"]:
            line = self.EVENT_PINGS[0].copy()
            del line[field_name]
            ret = clean_assa_event(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the numeric fields with invalid values
        for field_name in ["user_prefs", "profile_creation_date"]:
            line = self.EVENT_PINGS[0].copy()
            line[field_name] = 2 ** 32
            parts = clean_assa_event(line, self.params).next()
            self.assertEquals(parts[field_name], -1)

        # test the filter on the numeric fields with invalid values
        for field_name in ["user_prefs", "profile_creation_date"]:
            line = self.EVENT_PINGS[0].copy()
            line[field_name] = 100.4
            parts = clean_assa_event(line, self.params).next()
            self.assertEquals(parts[field_name], 100)

        # test the filter on the optional fields
        for field_name in ['action_position', 'source', 'release_channel', 'shield_id']:
            line = self.EVENT_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_assa_event(line, self.params).next())

            # test on "null" values on optional key
            line = self.EVENT_PINGS[0].copy()
            line[field_name] = None
            parts = clean_assa_event(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

        # test the filter on the nested fields
        for field_name in ['value']:
            line = self.EVENT_PINGS[0].copy()

            # "value" should be serialized by JSON
            val = line["value"]
            parts = clean_assa_event(line, self.params).next()
            self.assertEqual(parts["value"], json.dumps(val))

            # missing "value" will be replaced by an empty dict
            del line["value"]
            parts = clean_assa_event(line, self.params).next()
            self.assertEqual(parts["value"], json.dumps({}))

            # non-dict "value" will throw
            line["value"] = "not_a_dict"
            ret = clean_assa_event(line, self.params)
            self.assertRaises(StopIteration, ret.next)

    def test_special_filter_for_assa_performance(self):
        line = self.PERFORMANCE_PINGS[0].copy()
        line["event"] = "PERSONALIZATION_V1_ITEM_RELEVANCE_SCORE_DURATION"
        ret = assa_performance_filter(line, self.params)
        self.assertRaises(StopIteration, ret.next)

    def test_clean_assa_performance(self):
        self.assertIsNotNone(clean_assa_performance(self.PERFORMANCE_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "addon_version", "event"]:
            line = self.PERFORMANCE_PINGS[0].copy()
            del line[field_name]
            ret = clean_assa_performance(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ["page", "source", "event_id", "session_id", "release_channel",
                           "shield_id"]:
            line = self.PERFORMANCE_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_assa_performance(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_assa_performance(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

        # test the filter on the numeric fields with invalid values
        for field_name in ["value", "user_prefs"]:
            line = self.PERFORMANCE_PINGS[0].copy()
            line[field_name] = 2 ** 32
            parts = clean_assa_performance(line, self.params).next()
            self.assertEquals(parts[field_name], -1)

        # test the filter on the numeric fields with float
        for field_name in ["value", "user_prefs"]:
            line = self.PERFORMANCE_PINGS[0].copy()
            line[field_name] = 100.4
            parts = clean_assa_performance(line, self.params).next()
            self.assertEquals(parts[field_name], 100)

    def test_clean_assa_masga(self):
        self.assertIsNotNone(clean_assa_masga(self.MASGA_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "event"]:
            line = self.MASGA_PINGS[0].copy()
            del line[field_name]
            ret = clean_assa_masga(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ["page", "source", "session_id", "release_channel",
                           "shield_id", "addon_version"]:
            line = self.MASGA_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_assa_masga(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_assa_masga(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

        # test the filter on the numeric fields with invalid values
        for field_name in ["value", "user_prefs"]:
            line = self.MASGA_PINGS[0].copy()
            line[field_name] = 2 ** 32
            ret = clean_assa_masga(line, self.params)
            self.assertEqual(ret.next()[field_name], -1)

        # test the filter on the numeric fields with float
        for field_name in ["value", "user_prefs"]:
            line = self.MASGA_PINGS[0].copy()
            line[field_name] = 100.4
            parts = clean_assa_masga(line, self.params).next()
            self.assertEquals(parts[field_name], 100)

    def test_clean_assa_impression(self):
        self.assertIsNotNone(clean_assa_impression(self.IMPRESSION_PINGS[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "addon_version", "page"]:
            line = self.IMPRESSION_PINGS[0].copy()
            del line[field_name]
            ret = clean_assa_impression(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ["source", "release_channel", "shield_id", "region"]:
            line = self.IMPRESSION_PINGS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_assa_impression(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_assa_impression(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

        # test the filter on the numeric fields with invalid values
        for field_name in ["user_prefs"]:
            line = self.IMPRESSION_PINGS[0].copy()
            line[field_name] = 2 ** 32
            ret = clean_assa_impression(line, self.params)
            self.assertEqual(ret.next()[field_name], -1)

        # test the filter on the numeric fields with float
        for field_name in ["user_prefs"]:
            line = self.IMPRESSION_PINGS[0].copy()
            line[field_name] = 100.4
            parts = clean_assa_impression(line, self.params).next()
            self.assertEquals(parts[field_name], 100)

        # test the client_id gets overwritten by impression_id
        line = self.SESSION_PINGS[0].copy()
        line["client_id"] = "n/a"
        line["impression_id"] = "some_impression_id"
        parts = clean_assa_impression(line, self.params).next()
        self.assertEqual(parts["client_id"], line["impression_id"])
