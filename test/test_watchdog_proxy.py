import unittest
import random
import uuid
import time
import datetime
import sys
from itertools import combinations

from infernyx.rule_helpers import watchdog_proxy_events_filter, clean_watchdog_proxy_event

UUID = [str(uuid.uuid4()) for _i in range(10)]
IP = ["15.211.153.0", "10.192.171.13"]
UA = ["python-requests/2.9.1"]


def generate_new_item():
    payload = {
        "event": "new_item",
        "consumer_name": "screenshots",
        "watchdog_id": random.choice(UUID),
        "type": "image/png"
    }
    return payload


def generate_poller_heartbeat():
    payload = {
        "event": "poller_heartbeat",
        "poller_id": random.choice(UUID),
        "items_in_queue": random.randint(0, 100),
        "items_in_progress": random.randint(0, 100),
        "items_in_waiting": random.randint(0, 100)
    }
    return payload


def generate_worker_works():
    payload = {
        "event": "worker_works",
        "consumer_name": "screenshots",
        "worker_id": random.choice(UUID),
        "watchdog_id": random.choice(UUID),
        "photodna_tracking_id": random.choice(UUID),
        "is_match": True,
        "is_error": True,
        "timing_sent": random.randint(0, 100),
        "timing_received": random.randint(0, 100),
        "timing_submitted": random.randint(0, 100)
    }
    return payload


def attach_extra_info(ping):
    ping["topic"] = "watchdog-proxy"
    ping["ip"] = random.choice(IP)
    ping["ua"] = random.choice(UA)
    now = datetime.datetime.utcnow()
    ping["date"] = now.strftime("%Y-%m-%d")
    ping["timestamp"] = time.time() * 1000
    return ping


class TestWatchdogProxy(unittest.TestCase):
    def setUp(self):
        self.params = {}
        super(TestWatchdogProxy, self).setUp()

        self.NEW_ITEM = [attach_extra_info(generate_new_item()) for i in range(5)]
        self.POLLER_HEARTBEAT = [attach_extra_info(generate_poller_heartbeat()) for i in range(5)]
        self.WORKER_WORKS = [attach_extra_info(generate_worker_works()) for i in range(5)]
        self.FIXTURE = self.NEW_ITEM + self.POLLER_HEARTBEAT + self.WORKER_WORKS

    def test_filters(self):
        n_event_logs = 0

        for line in self.FIXTURE:
            for _ in watchdog_proxy_events_filter(line, self.params):
                n_event_logs += 1

        self.assertEqual(n_event_logs, 15)

    def test_clean_watchdog_proxy_event(self):
        for event in self.FIXTURE:
            self.assertIsNotNone(clean_watchdog_proxy_event(event, self.params).next())

    def test_clean_watchdog_proxy_new_item(self):
        # test the filter on the required fields
        for field_name in ["event"]:
            line = self.NEW_ITEM[0].copy()
            del line[field_name]
            ret = clean_watchdog_proxy_event(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ['consumer_name', 'watchdog_id', 'type']:
            line = self.NEW_ITEM[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_watchdog_proxy_event(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_watchdog_proxy_event(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

    def test_clean_watchdog_proxy_poller_heartbeat(self):
        # test the filter on the required fields
        for field_name in ["event"]:
            line = self.POLLER_HEARTBEAT[0].copy()
            del line[field_name]
            ret = clean_watchdog_proxy_event(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ['poller_id']:
            line = self.POLLER_HEARTBEAT[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_watchdog_proxy_event(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_watchdog_proxy_event(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

        # test the filter on the numeric fields with invalid values
        for field_name in ["items_in_queue", "items_in_progress", "items_in_waiting"]:
            line = self.POLLER_HEARTBEAT[0].copy()
            line[field_name] = 2 ** 32
            parts = clean_watchdog_proxy_event(line, self.params).next()
            self.assertEquals(parts[field_name], -1)

        # test the filter on the numeric fields with float
        for field_name in ["items_in_queue", "items_in_progress", "items_in_waiting"]:
            line = self.POLLER_HEARTBEAT[0].copy()
            line[field_name] = 100.4
            parts = clean_watchdog_proxy_event(line, self.params).next()
            self.assertEquals(parts[field_name], 100)

    def test_clean_watchdog_proxy_worker_works(self):
        # test the filter on the required fields
        for field_name in ["event"]:
            line = self.WORKER_WORKS[0].copy()
            del line[field_name]
            ret = clean_watchdog_proxy_event(line, self.params)
            self.assertRaises(StopIteration, ret.next)

        # test the filter on the optional fields
        for field_name in ['worker_id', 'consumer_name', 'watchdog_id', 'photodna_tracking_id']:
            line = self.WORKER_WORKS[0].copy()
            del line[field_name]
            self.assertIsNotNone(clean_watchdog_proxy_event(line, self.params).next())

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_watchdog_proxy_event(line, self.params).next()
            self.assertEqual(parts[field_name], "n/a")

        # test the filter on the numeric fields with invalid values
        for field_name in ["timing_sent", "timing_received", "timing_submitted"]:
            line = self.WORKER_WORKS[0].copy()
            line[field_name] = 2 ** 32
            parts = clean_watchdog_proxy_event(line, self.params).next()
            self.assertEquals(parts[field_name], -1)

        # test the filter on the numeric fields with float
        for field_name in ["timing_sent", "timing_received", "timing_submitted"]:
            line = self.WORKER_WORKS[0].copy()
            line[field_name] = 100.4
            parts = clean_watchdog_proxy_event(line, self.params).next()
            self.assertEquals(parts[field_name], 100)

        # test the filter on the optional boolean fields
        for field_name in ['is_match', 'is_error']:
            line = self.WORKER_WORKS[0].copy()
            del line[field_name]
            parts = clean_watchdog_proxy_event(line, self.params).next()
            self.assertFalse(parts[field_name])

            # test on "null" values on optional key
            line[field_name] = None
            parts = clean_watchdog_proxy_event(line, self.params).next()
            self.assertFalse(parts[field_name])
