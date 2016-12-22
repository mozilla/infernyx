import unittest
from mock import Mock
from infernyx.rules import parse_tiles
from infernyx.rules import parse_ip
from infernyx.rules import clean_data
import re


class TestParseTiles(unittest.TestCase):
    def setUp(self):
        pass

    def test_parse_tiles_newtabs(self):
        imps = [
            {u'tiles': [{}, {u'id': 518}, {u'id': 519}, {u'id': 594}, {u'id': 520}, {u'id': 521}, {u'id': 522},
                            {u'id': 523}, {u'id': 524}, {u'id': 525}, {u'id': 526}, {}, {}],
             u'locale': u'pt-BR', u'ip': u'124.179.24.69', u'timestamp': 1420648352586, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0', u'view': 5},
            {u'tiles': [{}], u'locale': u'ru', u'ip': u'170.79.137.117', u'timestamp': 1420648352583,
             u'date': u'2015-01-07', u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0',
             u'block': 0},
            {u'tiles': [{}, {u'id': 518}, {u'id': 519}, {u'id': 594}, {u'id': 520}, {u'id': 521}, {u'id': 522},
                            {u'id': 523}, {u'id': 524}, {u'id': 525}, {u'id': 526}, {}, {}],
             u'locale': u'pt-BR', u'ip': u'124.179.24.69', u'timestamp': 1420648352586, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0', u'view': 5}
        ]
        newtabs = [1, 0, 1]
        for imp, newtab in zip(imps, newtabs):
            count_newtabs = 0
            for tile in parse_tiles(imp, None):
                count_newtabs += tile.get('newtabs', 0)
            self.assertEqual(count_newtabs, newtab)

    def test_parse_tiles_actions(self):
        oimp = {u'tiles': [{}, {u'id': 518}, {u'id': 519}, {u'id': 594}, {u'id': 520}, {u'id': 521}, {u'id': 522},
                               {u'id': 523}, {u'id': 524}, {u'id': 525}, {u'id': 526}, {}, {}],
                u'locale': u'pt-BR', u'ip': u'124.179.24.69', u'timestamp': 1420648352586, u'date': u'2015-01-07',
                u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0', u'view': 3}

        actions = ['click', 'pin', 'block', 'sponsored', 'sponsored_link']
        verifies = ['clicks', 'pinned', 'blocked', 'sponsored', 'sponsored_link']
        for action, verify in zip(actions, verifies):
            imp = oimp.copy()
            imp[action] = 2
            total_tiles = 0
            action_tiles = 0
            for tile in parse_tiles(imp, None):
                total_tiles += 1
                action_tiles += tile.get(verify, 0)
            self.assertEqual(total_tiles, 1)
            self.assertEqual(action_tiles, 1)

    def test_parse_tiles_impression(self):
        imps = [
            {u'tiles': [{}, {u'id': 518}, {u'id': 519}, {u'id': 594}, {u'id': 520}, {u'id': 521}, {u'id': 522},
                            {u'id': 523}, {u'id': 524}, {u'id': 525}, {u'id': 526}, {}, {}],
             u'locale': u'pt-BR', u'ip': u'124.179.24.69', u'timestamp': 1420648352586, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0', u'view': 5},
            {u'tiles': [{}], u'locale': u'ru', u'ip': u'170.79.137.117', u'timestamp': 1420648352583,
             u'date': u'2015-01-07', u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0',
             u'view': 1},
            {u'tiles': [{}, {u'id': 518}, {u'id': 519}, {u'id': 594}, {u'id': 520}, {u'id': 521}, {u'id': 522},
                            {u'id': 523}, {u'id': 524}, {u'id': 525}, {u'id': 526}, {}, {}],
             u'locale': u'pt-BR', u'ip': u'124.179.24.69', u'timestamp': 1420648352586, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0', u'view': 5, u'click': 1}
        ]
        total_imps = [5, 0, 0]
        for imp, total_imp in zip(imps, total_imps):
            imp_count = 0
            for tile in parse_tiles(imp, None):
                if tile.get('tile_id') is not None:
                    imp_count += tile['impressions']

            self.assertEqual(total_imp, imp_count)

    def test_parse_tiles_urls(self):
        imps = [
            {u'tiles': [{}, {u'id': 518}, {u'id': 519}, {u'id': 594}, {u'id': 520}, {u'id': 521}, {u'id': 522},
                            {u'id': 523}, {u'id': 524}, {u'id': 525}, {u'id': 526}, {}, {}],
             u'locale': u'pt-BR', u'ip': u'124.179.24.69', u'timestamp': 1420648352586, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0', u'view': 5},
            {u'tiles': [{u'url': u'www.google.com.pk', u'score': 2}, {u'url': u'www.zalmos.com', u'score': 1},
                        {u'url': u''}, {u'url': u'en.wikipedia.org'}, {u'url': u'friv.com', u'id': 123}, {u'url': u''},
                        {u'url': u'apps.facebook.com'}, {u'url': u''}, {u'url': u'www.systemrequirementslab.com'},
                        {u'url': u'sufijhelumi.wordpress.com'}, {u'url': u'www.vidtomp3.com'}, {u'url': u''},
                        {u'url': u''}, {u'url': u'tune.pk'}], u'locale': u'en-US', u'ip': u'39.32.221.176',
             u'timestamp': 1420648371342, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0', u'click': 4},
            {u'tiles': [{u'url': u'www.google.com.pk', u'score': 2}, {u'url': u'www.zalmos.com', u'score': 1},
                        {u'url': u''}, {u'url': u'en.wikipedia.org'}, {u'url': u'friv.com', u'id': 123}, {u'url': u''},
                        {u'url': u'apps.facebook.com'}, {u'url': u''}, {u'url': u'www.systemrequirementslab.com'},
                        {u'url': u'sufijhelumi.wordpress.com'}, {u'url': u'www.vidtomp3.com'}, {u'url': u''},
                        {u'url': u''}, {u'url': u'tune.pk'}], u'locale': u'en-US', u'ip': u'39.32.221.176',
             u'timestamp': 1420648371342, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0', u'view': 4},
            {u'tiles': [{u'url': u''}, {u'url': u'scholar.google.es'}, {u'url': u'nlm.nih.gov'}, {u'id': 498},
                        {u'id': 499}, {u'id': 500}, {u'id': 501}, {u'id': 502}, {u'id': 503}, {u'id': 504},
                        {u'id': 505}, {u'id': 506}, {u'id': 507}, {u'url': u'nlm.nih.gov'},
                        {u'url': u'aviarioexposito.es.tl'}], u'locale': u'en-US', u'ip': u'169.158.176.146',
             u'timestamp': 1420648372134, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 6.1; rv:34.0) Gecko/20100101 Firefox/34.0', u'view': 7}

        ]
        total_imps = [0, 0, 9, 4]
        total_clicks = [0, 1, 0, 0]
        for imp, total_imp, total_click in zip(imps, total_imps, total_clicks):
            imp_count = 0
            click_count = 0
            for tile in parse_tiles(imp, None):
                if tile.get('url') is not None:
                    imp_count += tile['impressions']
                    click_count += tile['clicks']

            self.assertEqual(total_imp, imp_count)
            self.assertEqual(total_imp, imp_count)

    def test_double_ip(self):
        records = [
            {u'tiles': [{u'url': u'www.google.com.pk', u'score': 2}, {u'url': u'www.zalmos.com', u'score': 1},
                        {u'url': u''}, {u'url': u'en.wikipedia.org'}, {u'url': u'friv.com', u'id': 123}, {u'url': u''},
                        {u'url': u'apps.facebook.com'}, {u'url': u''}, {u'url': u'www.systemrequirementslab.com'},
                        {u'url': u'sufijhelumi.wordpress.com'}, {u'url': u'www.vidtomp3.com'}, {u'url': u''},
                        {u'url': u''}, {u'url': u'tune.pk'}], u'locale': u'en-US', u'ip': u'39.32.221.176, 169.158.176.146',
             u'timestamp': 1420648371342, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0', u'view': 4},
            {u'tiles': [{u'url': u'www.google.com.pk', u'score': 2}, {u'url': u'www.zalmos.com', u'score': 1},
                        {u'url': u''}, {u'url': u'en.wikipedia.org'}, {u'url': u'friv.com', u'id': 123}, {u'url': u''},
                        {u'url': u'apps.facebook.com'}, {u'url': u''}, {u'url': u'www.systemrequirementslab.com'},
                        {u'url': u'sufijhelumi.wordpress.com'}, {u'url': u'www.vidtomp3.com'}, {u'url': u''},
                        {u'url': u''}, {u'url': u'tune.pk'}], u'locale': u'en-US', u'ip': u'39.32.221.176',
             u'timestamp': 1420648371342, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0', u'view': 4}]

        for rec in records:
            new_rec = parse_ip(rec, None).next()
            self.assertIn('country_code', new_rec)

    def test_clean_data_double_ip(self):
        params = Mock()
        params.ip_pattern = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
        records = [
            {u'tiles': [{u'url': u'www.google.com.pk', u'score': 2}, {u'url': u'www.zalmos.com', u'score': 1},
                        {u'url': u''}, {u'url': u'en.wikipedia.org'}, {u'url': u'friv.com', u'id': 123}, {u'url': u''},
                        {u'url': u'apps.facebook.com'}, {u'url': u''}, {u'url': u'www.systemrequirementslab.com'},
                        {u'url': u'sufijhelumi.wordpress.com'}, {u'url': u'www.vidtomp3.com'}, {u'url': u''},
                        {u'url': u''}, {u'url': u'tune.pk'}], u'locale': u'en-US', u'ip': u'39.32.221.176, 169.158.176.146',
             u'timestamp': 1420648371342, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0', u'view': 4},
            {u'tiles': [{u'url': u'www.google.com.pk', u'score': 2}, {u'url': u'www.zalmos.com', u'score': 1},
                        {u'url': u''}, {u'url': u'en.wikipedia.org'}, {u'url': u'friv.com', u'id': 123}, {u'url': u''},
                        {u'url': u'apps.facebook.com'}, {u'url': u''}, {u'url': u'www.systemrequirementslab.com'},
                        {u'url': u'sufijhelumi.wordpress.com'}, {u'url': u'www.vidtomp3.com'}, {u'url': u''},
                        {u'url': u''}, {u'url': u'tune.pk'}], u'locale': u'en-US', u'ip': u'39.32.221.176',
             u'timestamp': 1420648371342, u'date': u'2015-01-07',
             u'ua': u'Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0', u'view': 4}]
        for rec in records:
            new_rec = clean_data(rec, params).next()
            self.assertIn('ip', new_rec)
