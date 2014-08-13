import csv
import sys
import os
import stat
import tempfile
import logging
import datetime

from inferno.lib.rule import chunk_json_stream
from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset

from functools import partial
from collections import namedtuple

AUTORUN = True


# this function inserts disco job results to the database
def insert_postgres(disco_iter, params, job_id, host, user, password):

    def connect(host='localhost', user='postgres', password=None):
        import psycopg2
        from psycopg2.extras import DictCursor

        connect_string = ("host=%s user=%s " % (host, user)) + ("password=%s" % password) if password else ''
        print "CONNECT: %s" % connect_string
        connection = psycopg2.connect(connect_string)
        return connection, connection.cursor(cursor_factory=DictCursor)

    def log(jid, msg, severity=logging.INFO):
        logging.log(severity, '%s: %s', jid, msg)

    def get_columns(kset):
        keys = kset['key_parts']
        values = kset['value_parts']
        return ','.join(keys[1:] + values)

    DataFile = namedtuple('DataFile', ['tempfile', 'tablename', 'columns'])

    selector = None
    csvwriter = None
    total_lines = 0
    connection, cursor = connect(host, user, password)
    datafiles = []

    query = "COPY %s (%s) FROM '%s' WITH DELIMITER '|'"
    try:
        for key, value in disco_iter:
            # New keyset was discovered
            if selector != key[0]:
                selector = key[0]
                keyset = params.keysets[selector]
                tmp = tempfile.NamedTemporaryFile(delete=False, prefix=selector, dir='/tmp')
                os.chmod(tmp.name, stat.S_IROTH | stat.S_IRGRP | stat.S_IRUSR)
                csvwriter = csv.writer(tmp, delimiter='|', escapechar='\\', quoting=csv.QUOTE_NONE)
                datafiles.append(DataFile(tmp, keyset['table'], get_columns(keyset)))
                log(job_id, "Saving %s data in %s" % (keyset['table'], tmp.name))

            data = tuple(key[1:]) + tuple(value)
            escaped = [unicode(x).encode('unicode_escape') for x in data]
            # log(job_id, 'Debug.persist_results: %s' % escaped, logging.DEBUG)
            csvwriter.writerow(escaped)
            total_lines += 1

        for datafile in datafiles:
            # Close the tempfile descriptor
            datafile.tempfile.close()

            # Default delimiter is |, default escape is backslash
            command = query % (datafile.tablename, datafile.columns, datafile.tempfile.name)
            log(job_id, "Executing: %s" % command)

            cursor.execute(command)

    except Exception as e:
        log(job_id, "Error persisting results. Rolling back: %s" % e.message, logging.ERROR)
        import traceback
        trace = traceback.format_exc(15)
        log(job_id,  trace, logging.ERROR)
        connection.rollback()
        raise e
    else:
        connection.commit()
        log(job_id, "Processed %d records in %d keysets." % (total_lines, len(params.keysets)))
    finally:
        cursor.close()
        connection.close()
        for datafile in datafiles:
            try:
                if getattr(params, 'clean_db_files', True):
                    os.unlink(datafile.tempfile.name)
            except Exception as e:
                log(job_id, "Error removing temp file: %s." % e, logging.ERROR)
        sys.stdout.flush()


def millstodate(val):
    return datetime.datetime.fromtimestamp(long(val) / 1000).strftime('%Y-%m-%d')


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
            print "GROOVY: %s" % e
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


def parse_timestamp(parts, params):
    from datetime import datetime
    timestamp = parts.get('timestamp')
    try:
        parts['day'] = datetime.datetime.fromtimestamp(long(timestamp) / 1000).strftime('%Y-%m-%d')
    except:
        print "Error parsing timestamp: %s" % timestamp
        parts['day'] = datetime.today().strftime("%Y-%m-%d")
    yield parts


def parse_tiles(parts, params):
    """If we have a 'click', 'block' or 'pin' action, just emit one record,
        otherwise it's an impression, emit all of the records"""
    tiles = parts.get('tiles')

    position = None
    vals = {'clicks': 0, 'impressions': 0, 'pinned': 0, 'blocked': 0, 'fetches': 0}

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
        elif parts.get('fetch') is not None:
            position = parts['fetch']
            vals['fetches'] = 1
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
        parts_preprocess=[parse_ip, parse_ua, parse_timestamp, parse_tiles],
        result_processor=partial(insert_postgres,
                                 host='localhost',
                                 user='postgres',
                                 password='p@ssw0rd66'),
        combiner_function=combiner,
        keysets={
            'impression_stats': Keyset(
                key_parts=['day', 'position', 'locale', 'tile_id',
                           'country_code', 'os', 'browser', 'version', 'device'],
                value_parts=['impressions', 'clicks', 'pinned', 'blocked'],
                table='impression_stats_daily',
            ),
        },
    ),
]
