from inferno.lib.rule import chunk_json_stream
from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset
from infernyx.database import insert_postgres, insert_redshift
from infernyx.rules import impression_stats_init, parse_date, parse_locale, parse_ip, parse_ua, \
    combiner, RULES, clean_data, parse_tiles
from functools import partial
from config_infernyx import *

AUTO_RUN = False


def filter_enhanced(parts, params):
    if parts.get('enhanced'):
        yield parts


def count(parts, params):
    parts['count'] = 1
    yield parts

imp_rule = [r for r in RULES if r.name == 'impression_stats'][0]

# {"ver":"2","locale":"he-IL","ip":"37.46.38.244","date":"2014-12-03","timestamp":1417651151217,
# "action":"fetch_locale_unavailable",
# "ua":"Mozilla\/5.0 (X11; Ubuntu; Linux i686; rv:34.0) Gecko\/20100101 Firefox\/34.0"}

RULES = [
    # This rule will give enhanced
    InfernoRule(
        name='enhanced_stats',
        source_tags=['incoming:impression'],
        day_range=1,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        parts_preprocess=[clean_data, parse_date, parse_locale, parse_ip, parse_ua, parse_tiles, filter_enhanced],
        geoip_file=GEOIP,
        partitions=32,
        sort_buffer_size='25%',
        locale_whitelist={'ach', 'af', 'an', 'ar', 'as', 'ast', 'az', 'be', 'bg', 'bn-bd', 'bn-in', 'br', 'bs',
                          'ca', 'cs', 'csb', 'cy', 'da', 'de', 'el', 'en-gb', 'en-us', 'en-za', 'eo', 'es-ar',
                          'es-cl', 'es-es', 'es-mx', 'et', 'eu', 'fa', 'ff', 'fi', 'fr', 'fy-nl', 'ga-ie', 'gd',
                          'gl', 'gu-in', 'he', 'hi-in', 'hr', 'hu', 'hsb', 'hy-am', 'id', 'is', 'it', 'ja',
                          'ja-jp-mac', 'ka', 'kk', 'km', 'kn', 'ko', 'ku', 'lij', 'lt', 'lv', 'mai', 'mk', 'ml',
                          'mr', 'ms', 'my', 'nb-no', 'nl', 'nn-no', 'oc', 'or', 'pa-in', 'pl', 'pt-br', 'pt-pt',
                          'rm', 'ro', 'ru', 'si', 'sk', 'sl', 'son', 'sq', 'sr', 'sv-se', 'sw', 'ta', 'te', 'th',
                          'tr', 'uk', 'ur', 'vi', 'xh', 'zh-cn', 'zh-tw', 'zu'},
        combiner_function=combiner,
        keysets={
            'impression_stats': Keyset(
                key_parts=['date', 'locale', 'tile_id', 'country_code'],
                value_parts=['impressions', 'clicks', 'pinned', 'blocked', 'sponsored', 'sponsored_link'],
            ),
        },
    ),
    InfernoRule(
        name='application_stats',
        source_tags=['incoming:app'],
        day_range=1,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        parts_preprocess=[parse_date, parse_ip, parse_ua, count],
        geoip_file=GEOIP,
        partitions=32,
        sort_buffer_size='25%',
        combiner_function=combiner,
        key_parts=['date', 'locale', 'ver', 'country_code', 'action'],
        value_parts=['count'],
    ),
]



