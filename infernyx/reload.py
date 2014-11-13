from inferno.lib.rule import chunk_json_stream
from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset
from infernyx.database import insert_postgres, insert_redshift
from infernyx.rules import impression_stats_init, parse_date, parse_locale, parse_ip, parse_ua, combiner
from functools import partial
from config_infernyx import *

AUTO_RUN = False


def parse_tiles(parts, params):
    """Yield a single record, just for the newtabs"""

    vals = {'position': -1, 'tile_id': -1, 'clicks': 0, 'impressions': 0, 'pinned': 0, 'blocked': 0,
            'sponsored': 0, 'sponsored_link': 0, 'newtabs': 1}

    del parts['tiles']
    cparts = parts.copy()
    cparts.update(vals)
    yield cparts


def parse_timestamp(parts, params):
    import datetime

    timestamp = datetime.datetime.fromtimestamp(parts['timestamp'] / 1000.0)
    parts['minute'] = timestamp.hour * 60 + timestamp.minute
    parts['time'] = timestamp.strftime("%H:%M")
    parts['count'] = 1
    yield parts

RULES = [
    InfernoRule(
        name='backfill_newtabs',
        source_tags=['processed:impression'],
        day_range=1,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        parts_preprocess=[parse_date, parse_locale, parse_ip, parse_ua, parse_tiles],
        geoip_file=GEOIP,
        locale_whitelist={'ach', 'af', 'an', 'ar', 'as', 'ast', 'az', 'be', 'bg', 'bn-bd', 'bn-in', 'br', 'bs',
                          'ca', 'cs', 'csb', 'cy', 'da', 'de', 'el', 'en-gb', 'en-us', 'en-za', 'eo', 'es-ar',
                          'es-cl', 'es-es', 'es-mx', 'et', 'eu', 'fa', 'ff', 'fi', 'fr', 'fy-nl', 'ga-ie', 'gd',
                          'gl', 'gu-in', 'he', 'hi-in', 'hr', 'hu', 'hsb', 'hy-am', 'id', 'is', 'it', 'ja',
                          'ja-jp-mac', 'ka', 'kk', 'km', 'kn', 'ko', 'ku', 'lij', 'lt', 'lv', 'mai', 'mk', 'ml',
                          'mr', 'ms', 'my', 'nb-no', 'nl', 'nn-no', 'oc', 'or', 'pa-in', 'pl', 'pt-br', 'pt-pt',
                          'rm', 'ro', 'ru', 'si', 'sk', 'sl', 'son', 'sq', 'sr', 'sv-se', 'sw', 'ta', 'te', 'th',
                          'tr', 'uk', 'ur', 'vi', 'xh', 'zh-cn', 'zh-tw', 'zu'},
        # result_processor=partial(insert_postgres,
        #                          host='localhost',
        #                          database='mozsplice',
        #                          user='postgres',
        #                          password=PG_PASSWORD),
        result_processor=partial(insert_redshift,
                                 host=RS_HOST,
                                 port=5432,
                                 database=RS_DB,
                                 user=RS_USER,
                                 password=RS_PASSWORD,
                                 bucket_name=RS_BUCKET),
        combiner_function=combiner,
        keysets={
            'impression_stats': Keyset(
                key_parts=['date', 'position', 'locale', 'tile_id', 'country_code', 'os', 'browser',
                           'version', 'device', 'year', 'month', 'week'],
                value_parts=['impressions', 'clicks', 'pinned', 'blocked',
                             'sponsored', 'sponsored_link'],
                table='impression_stats_daily',
            ),
        },
    ),
    InfernoRule(
        name='rps',
        source_tags=['processed:impression'],
        day_range=1,
        map_input_stream=chunk_json_stream,
        parts_preprocess=[parse_timestamp],
        combiner_function=combiner,
        key_parts=['date', 'time'],
        value_parts=['count'],
    ),
]
