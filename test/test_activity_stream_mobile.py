import unittest
from itertools import combinations

from infernyx.rule_helpers import activity_stream_mobile_session_filter,\
    activity_stream_mobile_event_filter, clean_activity_stream_mobile_event,\
    clean_activity_stream_mobile_session


FIXTURE = [
    {"session_duration": 1876, "locale": "en_US", "ip": "18.155.180.200", "topic": "activity-stream-mobile-sessions", "date": "2017-02-15", "build": "1", "client_id": "80B461A9-D73D-4DC3-96C5-159BB83D1376", "timestamp": 1487171596958, "action": "ping_centre", "app_version": "7.0", "ua": "Client\/1 CFNetwork\/808.2.16 Darwin\/16.4.0", "release_channel": "release"},
    {"session_duration": 3442, "locale": "en_RO", "ip": "5.2.19.196", "topic": "activity-stream-mobile-sessions", "date": "2017-02-15", "build": "1", "client_id": "EA24374E-C8E6-4DBD-956A-90ACE72D3961", "timestamp": 1487170058336, "action": "ping_centre", "app_version": "7.0", "ua": "Client\/1 CFNetwork\/808.3 Darwin\/16.3.0", "release_channel": "release"},
    {"session_duration": 1475, "locale": "en_RO", "ip": "5.2.19.196", "topic": "activity-stream-mobile-sessions", "date": "2017-02-15", "build": "1", "client_id": "EA24374E-C8E6-4DBD-956A-90ACE72D3961", "timestamp": 1487170060289, "action": "ping_centre", "app_version": "7.0", "ua": "Client\/1 CFNetwork\/808.3 Darwin\/16.3.0", "release_channel": "release"},
    {"action_position": 4, "date": "2017-02-15", "locale": "en_US", "ip": "21.18.250.99", "event": "CLICK", "topic": "activity-stream-mobile-events", "source": "TOP_SITES", "build": "1", "client_id": "1EEB58C6-CFED-4BD7-A1BA-2F724ACF8361", "timestamp": 1487152909643, "action": "ping_centre", "app_version": "7.0", "ua": "Client\/1 CFNetwork\/808.2.16 Darwin\/16.1.0", "page": "NEW_TAB", "release_channel": "release"},
    {"action_position": 1, "date": "2017-02-15", "locale": "zh_CN", "ip": "11.203.167.23", "event": "CLICK", "topic": "activity-stream-mobile-events", "source": "TOP_SITES", "build": "1", "client_id": "2E069064-A122-4831-B757-0256E8C2D86E", "timestamp": 1487149656141, "action": "ping_centre", "app_version": "7.0", "ua": "Client\/1 CFNetwork\/808.0.2 Darwin\/16.0.0", "page": "NEW_TAB", "release_channel": "release"},
    {"action_position": 0, "date": "2017-02-15", "locale": "zh_CN", "ip": "11.203.167.23", "event": "CLICK", "topic": "activity-stream-mobile-events", "source": "TOP_SITES", "build": "1", "client_id": "2E069064-A122-4831-B757-0256E8C2D86E", "timestamp": 1487149666958, "action": "ping_centre", "app_version": "7.0", "ua": "Client\/1 CFNetwork\/808.0.2 Darwin\/16.0.0", "page": "NEW_TAB", "release_channel": "release"}
]


class TestActivityStreamMobile(unittest.TestCase):
    def setUp(self):
        self.params = {}
        super(TestActivityStreamMobile, self).setUp()

    def test_filters(self):
        n_session_logs = 0
        n_event_logs = 0

        for line in FIXTURE:
            for _ in activity_stream_mobile_session_filter(line, self.params):
                n_session_logs += 1

            for _ in activity_stream_mobile_event_filter(line, self.params):
                n_event_logs += 1
        self.assertEqual(n_session_logs, 3)
        self.assertEqual(n_event_logs, 3)

        # test filters are mutually exclusive
        n_total = 0
        for f1, f2 in combinations([activity_stream_mobile_event_filter,
                                    activity_stream_mobile_session_filter], 2):
            for line in FIXTURE:
                for item in f1(line, self.params):
                        for _ in f2(item, self.params):
                            n_total += 1
        self.assertEqual(n_total, 0)

    def test_clean_activity_stream_mobile_session(self):
        self.assertIsNotNone(clean_activity_stream_mobile_session(FIXTURE[0], self.params).next())

        ret = clean_activity_stream_mobile_session(FIXTURE[-1], self.params)
        self.assertRaises(StopIteration, ret.next)

        # test the filter on the required fields
        for field_name in ["client_id", "app_version", "build", "session_duration"]:
            line = FIXTURE[0].copy()
            del line[field_name]
            ret = clean_activity_stream_mobile_session(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ['release_channel']:
            line = FIXTURE[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_activity_stream_mobile_session(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_activity_stream_mobile_session(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

        # test the filter on the numeric fields with invalid values
        for field_name in ["session_duration"]:
            line = FIXTURE[0].copy()
            line[field_name] = -1000
            ret = clean_activity_stream_mobile_session(line, self.params)
            self.assertRaises(StopIteration, ret.next)

    def test_clean_activity_stream_mobile_event(self):
        self.assertIsNotNone(clean_activity_stream_mobile_event(FIXTURE[5], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "app_version", "page", "event"]:
            line = FIXTURE[-1].copy()
            del line[field_name]
            ret = clean_activity_stream_mobile_event(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ['action_position', 'source', 'release_channel']:
            line = FIXTURE[-1].copy()
            del line[field_name]
            self.assertIsNotNone(clean_activity_stream_mobile_event(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_activity_stream_mobile_event(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")
