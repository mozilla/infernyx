from datadog import statsd
from inferno.lib.rule import chunk_json_stream
from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset
from infernyx.database import insert_redshift, get_blacklist_ips, delete_old_blacklist_ips
import infernyx.rule_helpers
from functools import partial
from config_infernyx import *
import datetime
import logging

from infernyx.rule_helpers import clean_data, parse_date, parse_time, parse_locale, parse_ip, parse_ua,\
    parse_tiles, parse_urls, parse_distinct, parse_ip_clicks, count, filter_clicks, filter_blacklist,\
    create_timestamp_str, application_stats_filter, parse_batch,\
    activity_stream_mobile_session_filter, clean_activity_stream_mobile_session,\
    activity_stream_mobile_event_filter, clean_activity_stream_mobile_event,\
    assa_session_filter, assa_event_filter, assa_masga_filter, assa_performance_filter,\
    clean_assa_session, clean_assa_event, clean_assa_performance, clean_assa_masga,\
    timestamp_milli_to_micro, timestamp_micro_to_milli, assa_impression_filter,\
    clean_assa_impression, firefox_onboarding_session_filter, firefox_onboarding_event_filter,\
    clean_firefox_onboarding_event, clean_firefox_onboarding_session, ping_centre_main_filter,\
    clean_ping_centre_main, firefox_onboarding_session_filter_v2, firefox_onboarding_event_filter_v2,\
    clean_firefox_onboarding_session_v2, clean_firefox_onboarding_event_v2, validate_uuid4


log = logging.getLogger(__name__)
AUTORUN = True

LOCALE_WHITELIST = {'ach', 'af', 'an', 'ar', 'as', 'ast', 'az', 'be', 'bg', 'bn-bd', 'bn-in', 'br', 'bs',
                    'ca', 'cs', 'csb', 'cy', 'da', 'de', 'el', 'en-gb', 'en-us', 'en-za', 'eo', 'es-ar',
                    'es-cl', 'es-es', 'es-mx', 'et', 'eu', 'fa', 'ff', 'fi', 'fr', 'fy-nl', 'ga-ie', 'gd',
                    'gl', 'gu-in', 'he', 'hi-in', 'hr', 'hu', 'hsb', 'hy-am', 'id', 'is', 'it', 'ja',
                    'ja-jp-mac', 'ka', 'kk', 'km', 'kn', 'ko', 'ku', 'lij', 'lt', 'lv', 'mai', 'mk', 'ml',
                    'mr', 'ms', 'my', 'nb-no', 'nl', 'nn-no', 'oc', 'or', 'pa-in', 'pl', 'pt-br', 'pt-pt',
                    'rm', 'ro', 'ru', 'si', 'sk', 'sl', 'son', 'sq', 'sr', 'sv-se', 'sw', 'ta', 'te', 'th',
                    'tr', 'uk', 'ur', 'vi', 'xh', 'zh-cn', 'zh-tw', 'zu'}


def combiner(key, value, buf, done, params):
    if not done:
        i = len(value)
        buf[key] = [a + b for a, b in zip(buf.get(key, [0] * i), value)]
    else:
        return buf.iteritems()


def negative_impression_rule_init(params, host, port, database, user, password):
    delete_old_blacklist_ips(host, port, database, user, password)
    params.blacklisted_ips = get_blacklist_ips(host, port, database, user, password)


def impression_stats_init(input_iter, params):
    import geoip2.database
    import re
    try:
        geoip_file = params.geoip_file
    except Exception:
        geoip_file = './GeoLite2-Country.mmdb'
    params.geoip_db = geoip2.database.Reader(geoip_file)
    params.ip_pattern = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")


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


def report_suspicious_ips(disco_iter, params, job_id, host=None, port=None, database=None, user=None, password=None,
                          bucket_name=None, db_insert_fn=insert_redshift):
    if db_insert_fn(disco_iter, params, job_id, host=host, port=port, database=database,
                    user=user, password=password, bucket_name=bucket_name) > 0:
        msg = 'disco results %s | disco deref | ddfs xcat' % job_id
        statsd.event("Suspicious IPs detected.  To view IPs, run: ", msg)
        log.debug(msg)


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


RULES = [
    InfernoRule(
        name='impression_stats',
        source_tags=['incoming:impression'],
        min_blobs=IMPRESSION_MIN_BLOBS,
        max_blobs=IMPRESSION_MAX_BLOBS,
        archive=True,
        rule_cleanup=report_rule_stats,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        parts_preprocess=[clean_data, parse_date, parse_locale, parse_ip, parse_ua, parse_tiles],
        geoip_file=GEOIP,
        partitions=32,
        sort_buffer_size='25%',
        locale_whitelist=LOCALE_WHITELIST,
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
                           'version', 'device', 'year', 'month', 'week', 'enhanced', 'blacklisted'],
                value_parts=['impressions', 'clicks', 'pinned', 'blocked', 'sponsored', 'sponsored_link'],
                table='impression_stats_daily'),
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
                table='newtab_stats_daily')
        }
    ),
    InfernoRule(
        name='blacklisted_impression_stats',
        source_tags=['incoming:impression'],
        day_range=1,
        day_offset=1,
        time_delta={'oclock': 4},
        rule_cleanup=report_rule_stats,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        blacklisted=True,
        parts_preprocess=[clean_data, parse_date, parse_locale, parse_ip, filter_blacklist, parse_ua, parse_tiles],
        geoip_file=GEOIP,
        partitions=32,
        sort_buffer_size='25%',
        locale_whitelist=LOCALE_WHITELIST,
        rule_init_function=partial(negative_impression_rule_init,
                                   host=RS_HOST,
                                   port=RS_PORT,
                                   database=RS_DB,
                                   user=RS_USER,
                                   password=RS_PASSWORD),
        result_processor=partial(insert_redshift,
                                 host=RS_HOST,
                                 port=RS_PORT,
                                 database=RS_DB,
                                 user=RS_USER,
                                 password=RS_PASSWORD,
                                 bucket_name=RS_BUCKET),
        combiner_function=combiner,
        key_parts=['date', 'position', 'locale', 'tile_id', 'country_code', 'os', 'browser',
                   'version', 'device', 'year', 'month', 'week', 'enhanced', 'blacklisted'],
        value_parts=['impressions', 'clicks', 'pinned', 'blocked', 'sponsored', 'sponsored_link'],
        table='impression_stats_daily'
    ),
    InfernoRule(
        name='activity_stream_stats',
        source_tags=['incoming:activity_stream'],
        max_blobs=APP_MAX_BLOBS,
        min_blobs=APP_MIN_BLOBS,
        archive=True,
        rule_cleanup=report_rule_stats,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        parts_preprocess=[parse_batch, partial(clean_data, imps=False), parse_date, parse_ip,
                          parse_ua, create_timestamp_str],
        geoip_file=GEOIP,
        partitions=32,
        sort_buffer_size='25%',
        result_processor=partial(insert_redshift,
                                 host=RS_HOST,
                                 port=RS_PORT,
                                 database=RS_DB,
                                 user=RS_USER,
                                 password=RS_PASSWORD,
                                 bucket_name=RS_BUCKET),
        combiner_function=combiner,
        keysets={
            'activity_stream_session_stats': Keyset(
                key_parts=['client_id', 'addon_version', 'page', 'session_duration', 'session_id',
                           'load_trigger_type', 'load_trigger_ts', 'visibility_event_rcvd_ts', 'locale',
                           'topsites_first_painted_ts', 'user_prefs', 'release_channel', 'shield_id',
                           'is_preloaded', 'is_prerendered', 'topsites_data_late_by_ms', 'highlights_data_late_by_ms',
                           'screenshot_with_icon', 'screenshot', 'tippytop', 'rich_icon', 'no_image',
                           'topsites_pinned', 'profile_creation_date', 'region', 'custom_screenshot',
                           'receive_at', 'date', 'country_code', 'os', 'browser', 'version', 'device'],
                value_parts=[],  # no value_parts for this keyset
                # convert the timestamps to avoid the precision loss during the map/reduce
                parts_preprocess=[assa_session_filter,
                                  partial(validate_uuid4, fields=["client_id", "session_id"]),
                                  clean_assa_session,
                                  partial(timestamp_milli_to_micro, columns=['load_trigger_ts', 'visibility_event_rcvd_ts', 'topsites_first_painted_ts'])],
                parts_postprocess=[partial(timestamp_micro_to_milli, columns=['load_trigger_ts', 'visibility_event_rcvd_ts', 'topsites_first_painted_ts'])],
                column_mappings={"region": "client_region"},
                table=['assa_sessions_daily', 'assa_sessions_daily_by_client_id'],
            ),
            'activity_stream_event_stats': Keyset(
                key_parts=['client_id', 'addon_version', 'source', 'session_id', 'page', 'action_position',
                           'event', 'locale', 'user_prefs', 'release_channel', 'shield_id',
                           'receive_at', 'date', 'country_code', 'os', 'browser', 'version', 'device'],
                value_parts=[],  # no value_parts for this keyset
                parts_preprocess=[assa_event_filter,
                                  partial(validate_uuid4, fields=["client_id", "session_id"]),
                                  clean_assa_event],
                table='assa_events_daily',
            ),
            'activity_stream_performance_stats': Keyset(
                key_parts=['client_id', 'addon_version', 'session_id', 'page', 'source', 'event', 'event_id',
                           'value', 'locale', 'user_prefs', 'release_channel', 'shield_id',
                           'receive_at', 'date', 'country_code', 'os', 'browser', 'version', 'device'],
                value_parts=[],  # no value_parts for this keyset
                parts_preprocess=[assa_performance_filter,
                                  partial(validate_uuid4, fields=["client_id"]),
                                  clean_assa_performance],
                table='assa_performance_daily',
            ),
            'activity_stream_masga_stats': Keyset(
                key_parts=['client_id', 'addon_version', 'source', 'session_id', 'page', 'event', 'value',
                           'locale', 'user_prefs', 'release_channel', 'shield_id',
                           'receive_at', 'date', 'country_code', 'os', 'browser', 'version', 'device'],
                value_parts=[],  # no value_parts for this keyset
                parts_preprocess=[assa_masga_filter,
                                  partial(validate_uuid4, fields=["client_id"]),
                                  clean_assa_masga],
                table='assa_masga_daily',
            ),
            'activity_stream_impression_stats': Keyset(
                key_parts=['client_id', 'addon_version', 'page', 'source', 'date', 'position', 'locale', 'tile_id',
                           'user_prefs', 'country_code', 'os', 'browser', 'version', 'device', 'blacklisted',
                           'release_channel', 'shield_id', 'hour', 'minute', 'region'],
                value_parts=['impressions', 'clicks', 'pinned', 'blocked', 'pocketed'],
                parts_preprocess=[assa_impression_filter,
                                  partial(validate_uuid4, fields=["client_id"]),
                                  clean_assa_impression, parse_tiles, parse_time],
                column_mappings={"region": "client_region"},
                table='assa_impression_stats_daily'
            )
        }
    ),
    InfernoRule(
        name='application_stats',
        source_tags=['incoming:app'],
        max_blobs=APP_MAX_BLOBS,
        min_blobs=APP_MIN_BLOBS,
        archive=True,
        rule_cleanup=report_rule_stats,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        parts_preprocess=[parse_batch, partial(clean_data, imps=False), parse_date, parse_ip, parse_ua],
        geoip_file=GEOIP,
        partitions=32,
        sort_buffer_size='25%',
        result_processor=partial(insert_redshift,
                                 host=RS_HOST,
                                 port=RS_PORT,
                                 database=RS_DB,
                                 user=RS_USER,
                                 password=RS_PASSWORD,
                                 bucket_name=RS_BUCKET),
        combiner_function=combiner,
        keysets={
            'application_stats': Keyset(
                key_parts=['date', 'locale', 'ver', 'country_code', 'action', 'month',
                           'week', 'year', 'os', 'browser', 'version', 'device'],
                value_parts=['count'],
                parts_preprocess=[application_stats_filter, count],
                table='application_stats_daily',
            )
        }
    ),
    InfernoRule(
        name='ping_centre_stats',
        source_tags=['incoming:ping_centre'],
        max_blobs=APP_MAX_BLOBS,
        min_blobs=APP_MIN_BLOBS,
        archive=True,
        rule_cleanup=report_rule_stats,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        parts_preprocess=[parse_batch, partial(clean_data, imps=False), parse_date, parse_ip, parse_ua, create_timestamp_str],
        geoip_file=GEOIP,
        partitions=32,
        sort_buffer_size='25%',
        result_processor=partial(insert_redshift,
                                 host=RS_HOST,
                                 port=RS_PORT,
                                 database=RS_DB,
                                 user=RS_USER,
                                 password=RS_PASSWORD,
                                 bucket_name=RS_BUCKET),
        keysets={
            'ping_centre_main': Keyset(
                key_parts=['client_id', 'shield_id', 'event', 'value', 'release_channel', 'receive_at',
                           'locale', 'date', 'country_code', 'os', 'browser', 'version', 'device'],
                value_parts=[],  # no value_parts for this keyset
                parts_preprocess=[ping_centre_main_filter,
                                  partial(validate_uuid4, fields=["client_id"]),
                                  clean_ping_centre_main],
                table='ping_centre_main',
            ),
            'activity_stream_mobile_session_stats': Keyset(
                key_parts=['client_id', 'build', 'app_version', 'session_duration', 'receive_at',
                           'locale', 'date', 'country_code', 'os', 'browser', 'version', 'device',
                           'release_channel'],
                value_parts=[],  # no value_parts for this keyset
                parts_preprocess=[activity_stream_mobile_session_filter, clean_activity_stream_mobile_session],
                table='activity_stream_mobile_stats_daily',
            ),
            'activity_stream_mobile_event_stats': Keyset(
                key_parts=['action_position', 'date', 'event', 'source', 'build', 'client_id', 'receive_at',
                           'app_version', 'locale', 'page', 'country_code', 'os', 'browser', 'version', 'device',
                           'release_channel'],
                value_parts=[],  # no value_parts for this keyset
                parts_preprocess=[activity_stream_mobile_event_filter, clean_activity_stream_mobile_event],
                table='activity_stream_mobile_events_daily',
            ),
            'firefox_onboarding_session_stats': Keyset(
                key_parts=['client_id', 'addon_version', 'session_begin', 'session_end', 'session_id', 'page',
                           'event', 'category', 'tour_source', 'tour_type',
                           'receive_at', 'locale', 'date', 'country_code', 'os', 'browser', 'version', 'device'],
                value_parts=["impression"],
                parts_preprocess=[firefox_onboarding_session_filter,
                                  partial(validate_uuid4, fields=["client_id"]),
                                  clean_firefox_onboarding_session],
                table='firefox_onboarding_sessions_daily',
            ),
            'firefox_onboarding_events_stats': Keyset(
                key_parts=['client_id', 'addon_version', 'session_id', 'page', 'event', 'tour_id', 'category',
                           'receive_at', 'locale', 'date', 'country_code', 'os', 'browser', 'version', 'device',
                           'timestamp', 'tour_type', 'tour_source', 'bubble_state', 'notification_state'],
                value_parts=["impression"],
                parts_preprocess=[firefox_onboarding_event_filter,
                                  partial(validate_uuid4, fields=["client_id"]),
                                  clean_firefox_onboarding_event],
                table='firefox_onboarding_events_daily',
            ),
            'firefox_onboarding_session2_stats': Keyset(
                key_parts=['client_id', 'addon_version', 'session_begin', 'session_end', 'session_id', 'page',
                           'parent_session_id', 'root_session_id', 'category', 'tour_type', 'type', 'release_channel',
                           'receive_at', 'locale', 'date', 'country_code', 'os', 'browser', 'version', 'device'],
                value_parts=[],  # no value_parts for this keyset
                parts_preprocess=[firefox_onboarding_session_filter_v2,
                                  partial(validate_uuid4, fields=["client_id"]),
                                  clean_firefox_onboarding_session_v2],
                table='firefox_onboarding_sessions2_daily',
            ),
            'firefox_onboarding_events2_stats': Keyset(
                key_parts=['client_id', 'addon_version', 'bubble_state', 'category', 'current_tour_id', 'logo_state',
                           'notification_impression', 'notification_state', 'page', 'parent_session_id', 'root_session_id',
                           'timestamp', 'tour_type', 'type', 'width', 'target_tour_id', 'release_channel',
                           'receive_at', 'locale', 'date', 'country_code', 'os', 'browser', 'version', 'device'],
                value_parts=[],  # no value_parts for this keyset
                parts_preprocess=[firefox_onboarding_event_filter_v2,
                                  partial(validate_uuid4, fields=["client_id"]),
                                  clean_firefox_onboarding_event_v2],
                table='firefox_onboarding_events2_daily',
            )
        },
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

        result_tag='incoming:site_tuples',
    ),
    InfernoRule(
        name='ip_click_counter',
        source_tags=['incoming:impression'],

        # run this job every day at 2am on yesterday's data
        day_range=1,
        day_offset=1,
        time_delta={'oclock': 2},

        map_input_stream=chunk_json_stream,
        parts_preprocess=[clean_data, parse_date, parse_ip_clicks],
        parts_postprocess=[partial(filter_clicks, click_threshold=1000, impression_threshold=10000)],
        result_processor=partial(report_suspicious_ips,
                                 host=RS_HOST,
                                 port=RS_PORT,
                                 database=RS_DB,
                                 user=RS_USER,
                                 password=RS_PASSWORD,
                                 bucket_name=RS_BUCKET),
        partitions=32,
        sort_buffer_size='25%',
        combiner_function=combiner,
        key_parts=['date', 'ip'],
        value_parts=['impressions', 'clicks'],
        column_mappings={'impressions': None, 'clicks': None},
        table='blacklisted_ips'
    ),
]

# Attache the dependent modules for each rule
for rule in RULES:
    rule.required_modules = [
        ('infernyx', infernyx.__file__),
        ('infernyx.rule_helpers', infernyx.rule_helpers.__file__)
    ]
