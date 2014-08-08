import csv
import sys
import os
import tempfile
import logging
import datetime

from inferno.lib.rule import chunk_json_stream
from inferno.lib.rule import InfernoRule
from inferno.lib.rule import Keyset

from functools import partial
from collections import namedtuple

AUTORUN = True


def impression_stats_init(input_iter, params):
    pass


# this function inserts disco job results to the database
def insert_postgres(disco_iter, params, job_id, dbname, host, user, password):

    def connect(dbname, host='localhost', user='postgres', password=None):
        import psycopg2
        from psycopg2.extras import DictCursor

        connection = psycopg2.connect(("dbname=%s host=%s user=%s" % (dbname, host, user)) +
                                       "password=%s" if password else '')
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
    connection, cursor = connect(dbname, host, user, password)
    datafiles = []

    query = "COPY %s (%s) FROM '%s' WITH DELIMITER '|'"
    try:
        for key, value in disco_iter:
            # New keyset was discovered
            if selector != key[0]:
                selector = key[0]
                keyset = params.keysets[selector]
                tmp = tempfile.NamedTemporaryFile(delete=False, prefix=selector)
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


def combiner(key, value, buf, done, **kwargs):
    if not done:
        i = len(value)
        buf[key] = [a + b for a, b in zip(buf.get(key, [0] * i), value)]
    else:
        return buf.iteritems()


RULES = [
    InfernoRule(
        name='impressions',
        source_tags=['incoming:impression_stats'],
        archive=True,
        map_input_stream=chunk_json_stream,
        map_init_function=impression_stats_init,
        parts_preprocess=[parse_ip, parse_ua, parse_action, parse_timestamp],
        result_processor=partial(insert_postgres,
                                 dbname='impression_stats_daily',
                                 host='localhost',
                                 user='postgres',
                                 password='p@ssw0rd66'),
        combiner_function=combiner,
        keysets={
            'impression_stats': Keyset(
                key_parts=['day', 'position', 'locale', 'tile_id', 'country_code', 'os', 'browser', 'version'],
                value_parts=['impressions', 'clicks', 'pinned', 'blocked'],
                table='impression_stats',
            ),
        },
    ),
]
