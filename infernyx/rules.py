import datetime

from inferno.lib.rule import chunk_json_stream
from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset
from infernyx.database import insert_postgres, insert_redshift
from functools import partial
from config_infernyx import *

AUTORUN = True


def combiner(key, value, buf, done, params):
    if not done:
        i = len(value)
        buf[key] = [a + b for a, b in zip(buf.get(key, [0] * i), value)]
    else:
        return buf.iteritems()


def impression_stats_init(input_iter, params):
    from inferno.lib import settings
    import geoip2.database
    opts = settings.InfernoSettings()
    try:
        geoip_file = opts['geoip_db']
    except:
        try:
            geoip_file = params.geoip_file
        except Exception as e:
            # print "GROOVY: %s" % e
            geoip_file = './GeoLite2-Country.mmdb'
    params.geoip_db = geoip2.database.Reader(geoip_file)


def parse_ip(parts, params):
    ip = parts.get('ip', None)
    try:
        resp = params.geoip_db.country(parts['ip'])
        parts['country_code'] = resp.country.iso_code
    except:
        print "Error parsing ip address: %s" % ip
        parts['country_code'] = 'ERROR'
    yield parts


def parse_ua(parts, params):
    from ua_parser import user_agent_parser
    ua = parts.get('ua', None)
    try:
        result_dict = user_agent_parser.Parse(ua)
        parts['os'] = result_dict['os']['family']
        parts['version'] = "%s.%s" % (result_dict['user_agent']['major'], result_dict['user_agent']['minor'])
        parts['browser'] = result_dict['user_agent']['family']
        parts['device'] = result_dict['device']['family']
    except:
        print "Error parsing UA: %s" % ua
        parts.setdefault('os', 'n/a')
        parts.setdefault('version', 'n/a')
        parts.setdefault('browser', 'n/a')
        parts.setdefault('device', 'n/a')
    yield parts


def parse_tiles(parts, params):
    import sys
    """If we have a 'click', 'block' or 'pin' action, just emit one record,
        otherwise it's an impression, emit all of the records"""
    tiles = parts.get('tiles')

    position = None
    vals = {'clicks': 0, 'impressions': 0, 'pinned': 0, 'blocked': 0,
            'sponsored': 0, 'sponsored_link': 0}
    view = parts.get('view', sys.maxint)

    try:
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

        # emit all relavant tiles for this action
        del parts['tiles']
        for i, tile in enumerate(tiles):
            # print "Tile: %s" % str(tile)
            cparts = parts.copy()
            cparts.update(vals)

            # the position can be specified implicity or explicity
            if tile.get('pos') is not None:
                position = tile['pos']
            elif position is None:
                position = i
            cparts['position'] = position
            tile_id = tile.get('id')
            if tile_id is not None:
                cparts['tile_id'] = tile_id
            if position < view:
                yield cparts
    except:
        print "Error parsing tiles: %s" % str(tiles)


RULES = [
    InfernoRule(
        name='impression_stats',
        source_tags=['incoming:impression_stats'],
        archive=True,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        parts_preprocess=[parse_ip, parse_ua, parse_tiles],
        geoip_file=GEOIP,
        # result_processor=partial(insert_postgres,
        #                          host='localhost',
        #                          user='postgres',
        #                          password=PG_PASSWORD),
        result_processor=partial(insert_redshift,
                                 host=RS_HOST,
                                 port=5439,
                                 database='dev',
                                 user='postgres',
                                 password=RS_PASSWORD,
                                 key_id=AWS_KEY_ID,
                                 access_key=AWS_ACCESS_KEY,
                                 bucket_name="infernyx-redshift"),
        combiner_function=combiner,
        keysets={
            'impression_stats': Keyset(
                key_parts=['date', 'position', 'locale', 'tile_id',
                           'country_code', 'os', 'browser', 'version', 'device'],
                value_parts=['impressions', 'clicks', 'pinned', 'blocked',
                             'sponsored', 'sponsored_link'],
                table='impression_stats_daily',
            ),
        },
    ),
]
