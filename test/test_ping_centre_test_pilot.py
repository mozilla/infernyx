import unittest

import infernyx.rule_helpers as helpers


FIXTURE = [
    {"addon_id": "@testpilot-addon", "os_name": "Darwin", "date": "2017-02-09", "firefox_version": "53.0a2", "raw": "", "locale": "en-US", "ip": "51.13.0.67", "object": "@testpilot-addon", "addon_version": "1.1.0", "topic": "testpilot", "os_version": "15.6.0", "client_id": "a395815f-0aab-264c-b6f3-3ebfa7ac35e2", "timestamp": 1486681422881, "client_time": 187043, "ua": "Mozilla\/5.0 (Macintosh; Intel Mac OS X 10.11; rv: 53.0) Gecko\/20100101 Firefox\/53.0", "action": "ping_centre", "event_type": "disabled"}
]


class TestPingCentreTestPilot(unittest.TestCase):
    def setUp(self):
        self.params = {}
        super(TestPingCentreTestPilot, self).setUp()

    def test_filters(self):
        n_logs = 0

        for line in FIXTURE:
            for _ in helpers.ping_centre_test_pilot_filter(line, self.params):
                n_logs += 1

        self.assertEqual(n_logs, 1)

    def test_clean_ping_centre_test_pilot(self):
        self.assertIsNotNone(helpers.clean_ping_centre_test_pilot(FIXTURE[0], self.params).next())

        # test the filter on the required fields
        for field_name in ["client_id", "event_type", "client_time", "addon_id", "addon_version",
                           "firefox_version", "os_name", "os_version", "locale"]:
            line = FIXTURE[0].copy()
            del line[field_name]
            ret = helpers.clean_ping_centre_test_pilot(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the numeric fields with invalid values
        for field_name in ["client_time"]:
            line = FIXTURE[0].copy()
            line[field_name] = 2 ** 32
            ret = helpers.clean_ping_centre_test_pilot(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ['object', 'variants', 'raw']:
            line = FIXTURE[0].copy()
            del line[field_name]
            self.assertIsNotNone(helpers.clean_ping_centre_test_pilot(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = helpers.clean_ping_centre_test_pilot(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")
