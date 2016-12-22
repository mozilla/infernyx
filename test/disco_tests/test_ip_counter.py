import unittest
from mock import Mock
from infernyx.rule_helpers import parse_ip_clicks, filter_clicks
from infernyx.rules import report_suspicious_ips


def mocksert(disco_iter, params, job_id, **kwargs):
    return len(disco_iter)


class TestIPCounter(unittest.TestCase):
    def setUp(self):
        pass

    def test_parse_ip_clicks(self):
        imps = [
            {u'tiles': [{}, {u'id': 518}, {u'id': 519}, {u'id': 594}, {u'id': 520}, {u'id': 521}, {u'id': 522},
                            {u'id': 523}, {u'id': 524}, {u'id': 525}, {u'id': 526}, {}, {}],
             u'locale': u'pt-BR', u'ip': u'124.179.24.69', u'timestamp': 1420648352586, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0', u'click': 1},
            {u'tiles': [{u'url': u'www.google.com.pk', u'score': 2}, {u'url': u'www.zalmos.com', u'score': 1},
                        {u'url': u''}, {u'url': u'en.wikipedia.org'}, {u'url': u'friv.com', u'id': 518}, {u'url': u''},
                        {u'url': u'apps.facebook.com'}, {u'url': u''}, {u'url': u'www.systemrequirementslab.com'},
                        {u'url': u'sufijhelumi.wordpress.com'}, {u'url': u'www.vidtomp3.com'}, {u'url': u''},
                        {u'url': u''}, {u'url': u'tune.pk'}], u'locale': u'en-US', u'ip': u'39.32.221.176',
             u'timestamp': 1420648371342, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0', u'view': 4},
            {u'tiles': [{u'url': u'www.google.com.pk', u'score': 2}, {u'url': u'www.zalmos.com', u'score': 1},
                        {u'url': u''}, {u'url': u'en.wikipedia.org'}, {u'url': u'friv.com', u'id': 123}, {u'url': u''},
                        {u'url': u'apps.facebook.com'}, {u'url': u''}, {u'url': u'www.systemrequirementslab.com'},
                        {u'url': u'sufijhelumi.wordpress.com'}, {u'url': u'www.vidtomp3.com'}, {u'url': u''},
                        {u'url': u''}, {u'url': u'tune.pk'}], u'locale': u'en-US', u'ip': u'39.32.221.176',
             u'timestamp': 1420648371342, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0', u'click': 1},
            {u'tiles': [{u'url': u''}, {u'url': u'scholar.google.es'}, {u'url': u'nlm.nih.gov'},
                        {u'url': u'nlm.nih.gov'}, {u'url': u'aviarioexposito.es.tl'}], u'locale': u'en-US',
             u'ip': u'124.179.24.69', u'timestamp': 1420648372134, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0', u'block': 3},
            {u'tiles': [{u'url': u''}, {u'url': u'scholar.google.es'}, {u'url': u'nlm.nih.gov'}, {u'id': 498},
                        {u'id': 499}, {u'id': 500}, {u'id': 501}, {u'id': 502}, {u'id': 503}, {u'id': 504},
                        {u'id': 505}, {u'id': 506}, {u'id': 507}, {u'url': u'nlm.nih.gov'},
                        {u'url': u'aviarioexposito.es.tl'}], u'locale': u'en-US', u'ip': u'124.179.24.69',
             u'timestamp': 1420648372134, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0', u'block': 3},

        ]
        expect_to_pass = [True, True, True, False, True]
        for imp, expected in zip(imps, expect_to_pass):
            try:
                actual = parse_ip_clicks(imp, None).next() is not None
            except StopIteration:
                actual = False

            self.assertEqual(actual, expected)

    def test_filter_clicks(self):
        vals = [
            (('x', 'key1'), (5,0)),
            (('x', 'key2'), (15,0)),
            (('x', 'key3'), (50,0)),
            (('x', 'key4'), (150,0)),
            (('x', 'key5'), (81,0)),
            (('x', 'key6'), (25,0)),
            (('x', 'key7'), (5079333,0)),
        ]

        actuals = []
        expected = ['key3', 'key4', 'key5', 'key7']
        for key, val in vals:
            try:
                (_, k), _ = filter_clicks(key, val, None, impression_threshold=50).next()
                actuals.append(k)
            except StopIteration:
                pass
        self.assertEqual(actuals, expected)

    def test_filter_imps_and_clicks(self):
        vals = [
            (('x', 'key1'), (5,20)),
            (('x', 'key2'), (15,0)),
            (('x', 'key3'), (50,10)),
            (('x', 'key4'), (150,12)),
            (('x', 'key5'), (81,0)),
            (('x', 'key6'), (25,75)),
            (('x', 'key7'), (5079333,5660)),
        ]

        actuals = []
        expected = ['key1', 'key4', 'key5', 'key6', 'key7']
        for key, val in vals:
            try:
                (_, k), _ = filter_clicks(key, val, None, impression_threshold=80, click_threshold=20).next()
                actuals.append(k)
            except StopIteration:
                pass
        self.assertEqual(actuals, expected)

    def test_report_suspicious_ips(self):
        import infernyx.rules
        vals = [
            (('x', 'key1'), (5,25)),
            (('x', 'key2'), (15,17)),
            (('x', 'key3'), (50,0)),
            (('x', 'key4'), (150,9)),
            (('x', 'key5'), (81,22)),
            (('x', 'key6'), (25,33)),
            (('x', 'key7'), (5079333,0)),
        ]

        # this tests that statsd is called with all results
        def mock_event(*args, **kwargs):
            self.totals[0] += 1

        self.totals = [0]
        save = infernyx.rules.statsd.event
        infernyx.rules.statsd.event = Mock(side_effect=mock_event)
        report_suspicious_ips(vals, None, None, db_insert_fn=mocksert)
        infernyx.rules.statsd.event = save
        self.assertEqual(self.totals[0], 1)

    def test_dont_report_suspicious_ips(self):
        import infernyx.rules
        vals = [
        ]

        # this tests that statsd is called with all results
        def mock_event(*args, **kwargs):
            self.totals[0] += 1

        self.totals = [0]
        save = infernyx.rules.statsd.event
        infernyx.rules.statsd.event = Mock(side_effect=mock_event)
        report_suspicious_ips(vals, None, None, db_insert_fn=mocksert)
        infernyx.rules.statsd.event = save
        self.assertEqual(self.totals[0], 0)
