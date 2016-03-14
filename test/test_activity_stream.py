import unittest

from infernyx.rules import activity_stream_filter, application_stats_filter, clean_activity_stream


FIXTURE = [
    {"session_duration": 1591, "locale": "en-US", "ip": "10.192.171.13", "search": True, "date": "2016-03-04", "unload_reason": "close", "client_id": "3872bb4c-8cb2-4cf5-851e-503c29bca151", "max_scroll_depth": 713, "addon_version": "1.0.0", "total_history_size": 386, "ver": "3", "ua": "python-requests/2.9.1", "click_position": "100x100", "source": "top_sites", "timestamp": 1457142081000, "action": "activity_stream", "tab_id": 8, "load_reason": "newtab", "total_bookmarks": 14},
    {"session_duration": 1980, "locale": "en-US", "ip": "10.192.171.13", "search": False, "date": "2016-03-04", "unload_reason": "click", "client_id": "d1c08feb-1749-42a9-bd7d-2e77c091cdc4", "max_scroll_depth": 26, "addon_version": "1.0.0", "total_history_size": 714, "ver": "3", "ua": "python-requests/2.9.1", "click_position": "100x100", "source": "recent_bookmarks", "timestamp": 1457142081000, "action": "activity_stream", "tab_id": 5, "load_reason": "restore", "total_bookmarks": 5},
    {"session_duration": 952, "locale": "en-US", "ip": "10.192.171.13", "search": False, "date": "2016-03-04", "unload_reason": "unfocus", "client_id": "10cdbde8-3fd3-4616-98f2-f96b96b213e3", "max_scroll_depth": 126, "addon_version": "1.0.0", "total_history_size": 462, "ver": "3", "ua": "python-requests/2.9.1", "click_position": "100x100", "source": "spotlight", "timestamp": 1457142081000, "action": "activity_stream", "tab_id": 5, "load_reason": "focus", "total_bookmarks": 15},
    {"session_duration": 1545, "locale": "en-US", "ip": "10.192.171.13", "search": False, "date": "2016-03-04", "unload_reason": "click", "client_id": "bc07631a-6182-472c-bd3e-7a9030fc9fd0", "max_scroll_depth": 654, "addon_version": "1.0.0", "total_history_size": 309, "ver": "3", "ua": "python-requests/2.9.1", "click_position": "100x100", "source": "top_sites", "timestamp": 1457142081000, "action": "activity_stream", "tab_id": 2, "load_reason": "newtab", "total_bookmarks": 14},
    {"session_duration": 3773, "locale": "en-US", "ip": "15.211.153.0", "search": True, "date": "2016-03-04", "unload_reason": "search", "client_id": "3872bb4c-8cb2-4cf5-851e-503c29bca151", "max_scroll_depth": 735, "addon_version": "1.0.0", "total_history_size": 593, "ver": "3", "ua": "python-requests/2.9.1", "click_position": "100x100", "source": "top_sites", "timestamp": 1457142081000, "action": "activity_stream", "tab_id": 7, "load_reason": "newtab", "total_bookmarks": 19},
    {"ver": "3", "locale": "zu", "ip": "15.211.153.0", "date": "2016-02-18", "timestamp": 1455837962657, "action": "fetch_served", "ua": "python-requests/2.9.1", "channel": "hello"},
    {"ver": "3", "locale": "es-CL", "ip": "15.211.153.0", "date": "2016-02-18", "timestamp": 1455837962658, "action": "fetch_served", "ua": "python-requests/2.9.1", "channel": "aurora"},
    {"ver": "3", "locale": "ru", "ip": "15.211.153.0", "date": "2016-02-18", "timestamp": 1455837962661, "action": "fetch_served", "ua": "python-requests/2.9.1", "channel": "aurora"},
    {"ver": "2", "locale": "en-US", "ip": "15.211.153.0", "date": "2016-02-18", "timestamp": 1455837962662, "action": "fetch_served", "ua": "python-requests/2.9.1"},
    {"ver": "3", "locale": "es-MX", "ip": "15.211.153.0", "date": "2016-02-18", "timestamp": 1455837962665, "action": "fetch_served", "ua": "python-requests/2.9.1", "channel": "esr"}
]


class TestActivityStream(unittest.TestCase):
    def setUp(self):
        self.params = {}
        super(TestActivityStream, self).setUp()

    def test_filters(self):
        n_application_logs = 0
        for line in FIXTURE:
            for _ in application_stats_filter(line, self.params):
                n_application_logs += 1
        self.assertEqual(n_application_logs, 5)

        n_activity_stream_logs = 0
        for line in FIXTURE:
            for _ in activity_stream_filter(line, self.params):
                n_activity_stream_logs += 1
        self.assertEqual(n_activity_stream_logs, 5)

        n_total = 0
        for line in FIXTURE:
            for item in activity_stream_filter(line, self.params):
                for _ in application_stats_filter(item, self.params):
                    n_total += 1
        self.assertEqual(n_total, 0)

    def test_clean_activity_stream(self):
        self.assertIsNotNone(clean_activity_stream(FIXTURE[0], self.params).next())

        line = FIXTURE[0].copy()
        del line["client_id"]
        ret = clean_activity_stream(line, self.params)
        self.assertRaises(StopIteration, ret.next)

        line = FIXTURE[0].copy()
        del line["tab_id"]
        ret = clean_activity_stream(line, self.params)
        self.assertRaises(StopIteration, ret.next)

        line = FIXTURE[0].copy()
        line["session_duration"] = -1000
        ret = clean_activity_stream(line, self.params)
        self.assertRaises(StopIteration, ret.next)

        line = FIXTURE[0].copy()
        line["total_bookmarks"] = -1000
        ret = clean_activity_stream(line, self.params)
        self.assertRaises(StopIteration, ret.next)

        line = FIXTURE[0].copy()
        line["total_history_size"] = -1000
        ret = clean_activity_stream(line, self.params)
        self.assertRaises(StopIteration, ret.next)
