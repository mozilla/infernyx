import unittest
from mock import Mock
from infernyx.rules import parse_ip_clicks
from infernyx.rules import filter_clicks
from infernyx.rules import report_suspicious_ips

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
            {u'tiles': [{u'url': u''}, {u'url': u'scholar.google.es'}, {u'url': u'nlm.nih.gov'}, {u'id': 498},
                        {u'id': 499}, {u'id': 500}, {u'id': 501}, {u'id': 502}, {u'id': 503}, {u'id': 504},
                        {u'id': 505}, {u'id': 506}, {u'id': 507}, {u'url': u'nlm.nih.gov'},
                        {u'url': u'aviarioexposito.es.tl'}], u'locale': u'en-US', u'ip': u'124.179.24.69',
             u'timestamp': 1420648372134, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0', u'block': 3}

        ]
        expect_to_pass = [True, False, False, False]
        for imp, expected in zip(imps, expect_to_pass):
            try:
                actual = parse_ip_clicks(imp, None).next() is not None
            except StopIteration:
                actual = False

            self.assertEqual(actual, expected)

    def test_filter_clicks(self):
        vals = [
            (('x', 'key1'), (5,)),
            (('x', 'key2'), (15,)),
            (('x', 'key3'), (50,)),
            (('x', 'key4'), (150,)),
            (('x', 'key5'), (81,)),
            (('x', 'key6'), (25,)),
            (('x', 'key7'), (5079333,)),
        ]

        actuals = []
        expected = ['key3', 'key4', 'key5', 'key7']
        for key, val in vals:
            try:
                (_, k), _ = filter_clicks(key, val, None, threshold=50).next()
                actuals.append(k)
            except StopIteration:
                pass
        self.assertEqual(actuals, expected)

    def test_report_suspicious_ips(self):
        import infernyx.rules
        vals = [
            (('x', 'key1'), (5,)),
            (('x', 'key2'), (15,)),
            (('x', 'key3'), (50,)),
            (('x', 'key4'), (150,)),
            (('x', 'key5'), (81,)),
            (('x', 'key6'), (25,)),
            (('x', 'key7'), (5079333,)),
        ]

        # this tests that statsd is called with all results
        def mock_event(*args, **kwargs):
            self.totals[0] += 1

        self.totals = [0]
        save = infernyx.rules.statsd.event
        infernyx.rules.statsd.event = Mock(side_effect=mock_event)
        report_suspicious_ips(vals, None, None)
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
        report_suspicious_ips(vals, None, None)
        infernyx.rules.statsd.event = save
        self.assertEqual(self.totals[0], 0)
