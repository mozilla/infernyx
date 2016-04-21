"""
    create temporary table if not exists temp_imp_stats (like impression_stats_daily);

    delete from temp_imp_stats;

    insert into temp_imp_stats (tile_id, date, position, enhanced, locale, country_code, os,
                                browser, version, device, month, week, year, blacklisted,
                                impressions, clicks, pinned, blocked,
                                sponsored_link, sponsored) (
      select tile_id, date, position, enhanced, locale, country_code, os, browser, version,
        device, month, week, year, blacklisted,
        sum(impressions), sum(clicks), sum(pinned), sum(blocked),
        sum(sponsored_link), sum(sponsored)
      from impression_stats_daily
      where date = '2015-10-25'
      group by tile_id, date, position, enhanced, locale, country_code, os, browser, version,
        device, month, week, year, blacklisted
    );

    delete from impression_stats_daily where date = '2015-10-25';
    insert into impression_stats_daily select * from temp_imp_stats;
    delete from temp_imp_stats;
"""
from config_infernyx import *
import psycopg2
from psycopg2.extras import DictCursor
from optparse import OptionParser
import csv
import tempfile
import os
import boto
from boto.s3.key import Key
from boto.utils import compute_md5
import sys


def read_args():
    parser = OptionParser()
    return parser.parse_args()



def main():
    (options, args) = read_args()
    _, curr = _connect(RS_HOST, RS_PORT, RS_DB, DASH_USER, DASH_PASSWORD)

    for query_args, query in DATA_SETS:

        tmp = tempfile.NamedTemporaryFile(delete=False, prefix='dash', dir='/tmp')
        writer = csv.writer(tmp, delimiter=',', escapechar='\\', quoting=csv.QUOTE_NONE)
        writer.writerow(HEADER)

        print("Querying for: %s" % str(query_args))
        i = 0
        for record in _query(curr, query, query_args):
            writer.writerow(record)
            i += 1

        print("Got %d records in query" % i)

        tmp.flush()
        tmp.close()

        s3file = _upload_s3(tmp.name, DASH_KEY_ID, DASH_ACCESS_KEY, DASH_BUCKET,
                            "%s/%s.csv" % ('content-services-dashboard', '_'.join(query_args)))

        print("Uploaded %s" % s3file)
        os.unlink(tmp.name)
        sys.stdout.flush()

    print("done")

if __name__ == '__main__':
    main()