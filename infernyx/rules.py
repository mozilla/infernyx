from datadog import statsd
from inferno.lib.rule import chunk_json_stream
from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset
from infernyx.database import insert_redshift
from functools import partial
from config_infernyx import *
import datetime
import logging

log = logging.getLogger(__name__)
AUTORUN = True


def combiner(key, value, buf, done, params):
    if not done:
        i = len(value)
        buf[key] = [a + b for a, b in zip(buf.get(key, [0] * i), value)]
    else:
        return buf.iteritems()


def impression_stats_init(input_iter, params):
    import geoip2.database
    import re
    try:
        geoip_file = params.geoip_file
    except Exception as e:
        # print "GROOVY: %s" % e
        geoip_file = './GeoLite2-Country.mmdb'
    params.geoip_db = geoip2.database.Reader(geoip_file)
    params.ip_pattern = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")


def clean_data(parts, params, imps=True):
    import datetime
    try:
        if imps:
            assert parts['tiles'][0] is not None
        if getattr(params, 'ip_pattern', False):
            ip = parts['ip'].split(',')[0].strip()
            assert params.ip_pattern.match(ip)
        assert datetime.datetime.fromtimestamp(parts['timestamp'] / 1000.0)
        parts['locale'] = parts['locale'][:12]
        if parts.get('action'):
            parts['action'] = parts['action'][:254]
        yield parts

    except Exception as e:
        pass


def count(parts, params):
    parts['count'] = 1
    yield parts


def parse_date(parts, params):
    from datetime import datetime

    try:
        dt = datetime.strptime(parts['date'], "%Y-%m-%d")
        parts['year'] = dt.year
        parts['month'] = dt.month
        parts['week'] = dt.isocalendar()[1]
        yield parts
    except:
        pass


def parse_locale(parts, params):
    # skip illegal locales
    try:
        if parts['locale'].lower() in params.locale_whitelist:
            yield parts
    except:
        pass


def parse_ip(parts, params):
    ips = parts.get('ip', None)
    try:
        ip = ips.split(',')[0].strip()
        resp = params.geoip_db.country(ip)
        parts['country_code'] = resp.country.iso_code
    except Exception as e:
        # print "Error parsing ip address: %s %s" % (ips, e)
        parts['country_code'] = 'ERROR'
    yield parts


def parse_ua(parts, params):
    from ua_parser import user_agent_parser
    ua = parts.get('ua', None)
    try:
        result_dict = user_agent_parser.Parse(ua)
        parts['os'] = result_dict['os']['family'][:64]
        parts['version'] = ("%s.%s" % (result_dict['user_agent']['major'], result_dict['user_agent']['minor']))[:64]
        parts['browser'] = result_dict['user_agent']['family'][:64]
        parts['device'] = result_dict['device']['family'][:64]
    except:
        # print "Error parsing UA: %s" % ua
        parts.setdefault('os', 'n/a')
        parts.setdefault('version', 'n/a')
        parts.setdefault('browser', 'n/a')
        parts.setdefault('device', 'n/a')
    yield parts


def parse_tiles(parts, params):
    import sys
    from urlparse import urlparse
    """If we have a 'click', 'block' or 'pin' action, just emit one record,
        otherwise it's an impression, emit all of the records"""
    tiles = parts.get('tiles')

    position = None
    vals = {'clicks': 0, 'impressions': 0, 'pinned': 0, 'blocked': 0,
            'sponsored': 0, 'sponsored_link': 0, 'newtabs': 0, 'enhanced': False}
    view = parts.get('view', sys.maxint)

    try:

        # now prepare values for emitting this particular event
        if parts.get('click') is not None:
            position = parts['click']
            vals['clicks'] = 1
            tiles = [tiles[position]]
        elif parts.get('pin') is not None:
            position = parts['pin']
            vals['pinned'] = 1
            tiles = [tiles[position]]
        elif parts.get('block') is not None:
            position = parts['block']
            vals['blocked'] = 1
            tiles = [tiles[position]]
        elif parts.get('sponsored') is not None:
            position = parts['sponsored']
            vals['sponsored'] = 1
            tiles = [tiles[position]]
        elif parts.get('sponsored_link') is not None:
            position = parts['sponsored_link']
            vals['sponsored_link'] = 1
            tiles = [tiles[position]]
        else:
            vals['impressions'] = 1
            cparts = parts.copy()
            del cparts['tiles']
            cparts['newtabs'] = 1
            yield cparts

        del parts['tiles']

        # emit all relavant tiles for this action
        for i, tile in enumerate(tiles):
            # print "Tile: %s" % str(tile)
            cparts = parts.copy()
            cparts.update(vals)

            # the position can be specified implicity or explicity
            if tile.get('pos') is not None:
                slot = tile['pos']
            elif position is None:
                slot = i
            else:
                slot = position
            assert position < 1024
            cparts['position'] = slot

            url = tile.get('url')
            if url:
                cparts['enhanced'] = True
                cparts['url'] = url

            tile_id = tile.get('id')
            if tile_id is not None and isinstance(tile_id, int) and tile_id < 1000000 and slot <= view:
                cparts['tile_id'] = tile_id
            yield cparts
    except:
        print "Error parsing tiles: %s" % str(tiles)


def parse_urls(parts, params):
    def combos(arr):
        for i, ela in enumerate(arr):
            rest = arr[i:]
            for elb in rest:
                if elb < ela:
                    yield elb, ela
                else:
                    yield ela, elb

    # only process 'impression' records
    if "view" in parts:
        tiles = parts.get('tiles')
        date = parts.get('date')
        locale = parts.get('locale')
        country_code = parts.get('country_code')

        urls = set(tile.get('url') for tile in tiles if tile.get('url'))
        for url_a, url_b in combos(list(urls)):
            # print date, locale, country_code, url_a, url_b
            yield {'date': date, 'locale': locale, 'country_code': country_code, 'url_a': url_a, 'url_b': url_b,
                   'count': 1}


def parse_distinct(parts, params):
    if "view" in parts:
        tiles = parts.get('tiles')
        date = parts.get('date')
        locale = parts.get('locale')
        country_code = parts.get('country_code')
        urls = set(tile.get('url') for tile in tiles if tile.get('url'))
        yield {'date': date, 'locale': locale, 'country_code': country_code, 'distinct_urls': len(urls), 'count': 1}


def report_rule_stats(job):
    try:
        job_id = job.job_name
        rule_name = job.rule_name
        blobs = len(job.archiver.job_blobs)
        jobinfo = job.disco.jobinfo(job_id)
        status = jobinfo['active']
        timestamp = datetime.datetime.strptime(jobinfo['timestamp'], "%Y/%m/%d %H:%M:%S")
        now = datetime.datetime.now()
        diff = (now - timestamp).seconds * 1000
        statsd.incr("%s.blobs_processed" % rule_name, blobs)
        statsd.incr("%s.%s" % (rule_name, status))
        statsd.timer("%s.execution_time" % rule_name, diff)
        log.info("Wrote stats for: %s" % job_id)
    except Exception as e:
        log.error("Error writing stats %s" % e)


def report_suspicious_ips(it, params, job_id):
    total_ips = 0
    ips = []
    for (_, ip), (total,) in it:
        if total_ips >= 1000:
            break
        ips.append("%20s %d" % (ip, total))
        total_ips += 1

    if total_ips:
        msg = '\n'.join(ips)
        statsd.event("Suspicious IP report", msg)
        log.debug(msg)


def parse_ip_clicks(parts, params):
    tiles = parts.get('tiles')

    try:
        # now prepare values for emitting this particular event
        if parts.get('click') is not None:
            position = parts['click']
            tile = tiles[position]

            del parts['tiles']

            # print "Tile: %s" % str(tile)
            cparts = parts.copy()
            cparts['clicks'] = 1

            tile_id = tile.get('id')
            if tile_id is not None and isinstance(tile_id, int) and tile_id < 1000000:
                cparts['tile_id'] = tile_id
                yield cparts
    except Exception as e:
        print "Error parsing tiles: %s %s" % (e, str(tiles))


def tag_results(suffix, job):
    try:
        job_id = job.job_name
        date = job.archiver.tags[0].split(':')[-1]
        ddfs = job.ddfs

        # create tag name
        result_tag = "disco:results:%s" % job_id
        blobs = list(ddfs.blobs(result_tag))
        tag_name = suffix + date
        if len(blobs):
            log.info("Tagging %d results of job %s with tag %s" % (len(blobs), job_id, tag_name))
            ddfs.tag(tag_name, blobs)
        else:
            log.warn("No data to tag for job %s" % job_id)
    except Exception as e:
        log.error("Error tagging results %s" % e)


def filter_clicks(keys, vals, params, threshold=20):
    if vals[0] >= threshold:
        yield keys, vals


RULES = [
    InfernoRule(
        name='impression_stats',
        source_tags=['incoming:impression'],
        max_blobs=IMPRESSION_MAX_BLOBS,
        archive=True,
        rule_cleanup=report_rule_stats,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        parts_preprocess=[clean_data, parse_date, parse_locale, parse_ip, parse_ua, parse_tiles],
        geoip_file=GEOIP,
        partitions=32,
        sort_buffer_size='25%',
        min_blobs=IMPRESSION_MIN_BLOBS,
        locale_whitelist={'ach', 'af', 'an', 'ar', 'as', 'ast', 'az', 'be', 'bg', 'bn-bd', 'bn-in', 'br', 'bs',
                          'ca', 'cs', 'csb', 'cy', 'da', 'de', 'el', 'en-gb', 'en-us', 'en-za', 'eo', 'es-ar',
                          'es-cl', 'es-es', 'es-mx', 'et', 'eu', 'fa', 'ff', 'fi', 'fr', 'fy-nl', 'ga-ie', 'gd',
                          'gl', 'gu-in', 'he', 'hi-in', 'hr', 'hu', 'hsb', 'hy-am', 'id', 'is', 'it', 'ja',
                          'ja-jp-mac', 'ka', 'kk', 'km', 'kn', 'ko', 'ku', 'lij', 'lt', 'lv', 'mai', 'mk', 'ml',
                          'mr', 'ms', 'my', 'nb-no', 'nl', 'nn-no', 'oc', 'or', 'pa-in', 'pl', 'pt-br', 'pt-pt',
                          'rm', 'ro', 'ru', 'si', 'sk', 'sl', 'son', 'sq', 'sr', 'sv-se', 'sw', 'ta', 'te', 'th',
                          'tr', 'uk', 'ur', 'vi', 'xh', 'zh-cn', 'zh-tw', 'zu'},
        result_processor=partial(insert_redshift,
                                 host=RS_HOST,
                                 port=RS_PORT,
                                 database=RS_DB,
                                 user=RS_USER,
                                 password=RS_PASSWORD,
                                 bucket_name=RS_BUCKET),
        combiner_function=combiner,
        keysets={
            'impression_stats': Keyset(
                key_parts=['date', 'position', 'locale', 'tile_id', 'country_code', 'os', 'browser',
                           'version', 'device', 'year', 'month', 'week', 'enhanced'],
                value_parts=['impressions', 'clicks', 'pinned', 'blocked', 'sponsored', 'sponsored_link'],
                table='impression_stats_daily',
            ),
            'site_stats': Keyset(
                key_parts=['date', 'locale', 'country_code', 'os', 'browser', 'version', 'device', 'year',
                           'month', 'week', 'url'],
                value_parts=['impressions', 'clicks', 'pinned', 'blocked', 'sponsored', 'sponsored_link'],
                table='site_stats_daily',
            ),
            'newtab_stats': Keyset(
                key_parts=['date', 'locale', 'country_code', 'os', 'browser', 'version', 'device', 'year',
                           'month', 'week'],
                value_parts=['newtabs'],
                table='newtab_stats_daily',
            ),
        },
    ),
    InfernoRule(
        name='application_stats',
        source_tags=['incoming:app'],
        max_blobs=APP_MAX_BLOBS,
        archive=True,
        rule_cleanup=report_rule_stats,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        parts_preprocess=[partial(clean_data, imps=False), parse_date,
                          parse_ip, parse_ua, count],
        geoip_file=GEOIP,
        partitions=32,
        sort_buffer_size='25%',
        min_blobs=APP_MIN_BLOBS,
        result_processor=partial(insert_redshift,
                                 host=RS_HOST,
                                 port=RS_PORT,
                                 database=RS_DB,
                                 user=RS_USER,
                                 password=RS_PASSWORD,
                                 bucket_name=RS_BUCKET),
        combiner_function=combiner,
        key_parts=['date', 'locale', 'ver', 'country_code', 'action', 'month', 'week', 'year', 'os',
                   'browser', 'version', 'device'],
        value_parts=['count'],
        table='application_stats_daily',
    ),
    InfernoRule(
        name='site_tuples',
        source_tags=['processed:impression'],

        run=False,

        # process yesterday's data, today at 2am
        day_offset=1,
        day_range=1,
        time_delta={'oclock': 2},

        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        result_processor=None,
        parts_preprocess=[clean_data, parse_ip],
        geoip_file=GEOIP,
        partitions=32,
        sort_buffer_size='25%',
        combiner_function=combiner,

        keysets={
            'tuples': Keyset(
                key_parts=['date', 'locale', 'country_code', 'url_a', 'url_b'],
                value_parts=['count'],
                parts_preprocess=[parse_urls]),
            'distinct': Keyset(
                key_parts=['date', 'locale', 'country_code', 'distinct_urls'],
                value_parts=['count'],
                parts_preprocess=[parse_distinct])
        },

        # note that this rule_cleanup will be obsolete after the PR https://github.com/chango/inferno/pull/23
        # is merged and released, then only the 'result_tag' below will be required
        # result_tag='incoming:site_tuples',
        rule_cleanup=partial(tag_results, 'incoming:site_tuples:'),
        save=True,
        no_purge=True,
    ),
    InfernoRule(
        name='ip_click_counter',
        source_tags=['incoming:impression'],

        # run this job every day at 1am on yesterday's data
        day_range=1,
        day_offset=1,
        time_delta={'oclock': 1},

        map_input_stream=chunk_json_stream,
        parts_preprocess=[clean_data, parse_ip_clicks, count],
        parts_postprocess=[partial(filter_clicks, threshold=50)],
        result_processor=report_suspicious_ips,
        partitions=32,
        sort_buffer_size='25%',
        combiner_function=combiner,
        key_parts=['ip'],
        value_parts=['count'],
    ),
]
