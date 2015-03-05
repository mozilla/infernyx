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

# This program is meant to be called daily (2am UTC) by cron
# It will perform a set of queries, dump results to CSVs and upload them to the S3 bucket for the metrics dashboard


HEADER = ('date', 'locale', 'geo', 'value')

QUALITY_QUERY = """
select y.date, y.locale, y.country_code,
  case when y.clicks = 0 then 0 else (1000 - (y.blocked / y.clicks) * 10) end as %s
FROM
    (select x.date as date, x.locale as locale, x.country_code as country_code,
      sum(x.clicks) as clicks, sum(x.blocked) as blocked
    from
        (SELECT i.date as date,
          case when i.locale in ('zh-CN', 'en-GB', 'it', 'pl', 'pt-BR', 'ru', 'es-ES', 'fr', 'de', 'en-US')
            and i.country_code in ('US','CA','BR','MX','FR','ES','IT','PL','TR','RU','DE','IN','ID','CN','JP','GB')
            then i.country_code else 'other' end as country_code,
          case when i.locale in ('zh-CN', 'en-GB', 'it', 'pl', 'pt-BR', 'ru', 'es-ES', 'fr', 'de', 'en-US')
            and i.country_code in ('US','CA','BR','MX','FR','ES','IT','PL','TR','RU','DE','IN','ID','CN','JP','GB')
            then i.locale else 'all' end as locale,
          i.impressions as impressions,
          i.clicks as clicks,
          i.pinned as pinned,
          i.blocked as blocked
        FROM impression_stats_daily i
        JOIN tiles t on i.tile_id = t.id
        WHERE i.date < current_date) x
    group by x.date, x.locale, x.country_code
    order by x.date, x.locale, x.country_code) y
"""

BASE_QUERY = """
select x.date as date, x.locale as locale, x.country_code as country_code, sum(x.%s) as value
from
(SELECT i.date as date,
  case when i.locale in ('zh-CN', 'en-GB', 'it', 'pl', 'pt-BR', 'ru', 'es-ES', 'fr', 'de', 'en-US')
    and i.country_code in ('US','CA','BR','MX','FR','ES','IT','PL','TR','RU','DE','IN','ID','CN','JP','GB')
    then i.country_code else 'other' end as country_code,
  case when i.locale in ('zh-CN', 'en-GB', 'it', 'pl', 'pt-BR', 'ru', 'es-ES', 'fr', 'de', 'en-US')
    and i.country_code in ('US','CA','BR','MX','FR','ES','IT','PL','TR','RU','DE','IN','ID','CN','JP','GB')
    then i.locale else 'all' end as locale,
  i.impressions as impressions,
  i.clicks as clicks,
  i.pinned as pinned,
  i.blocked as blocked,
  case when i.clicks = 0 then 0 else (1000 - (i.blocked / i.clicks) * 10) end as quality
FROM impression_stats_daily i
JOIN tiles t on i.tile_id = t.id
WHERE t.type = '%s' and i.date < current_date) x
group by x.date, x.locale, x.country_code
order by x.date, x.locale, x.country_code
"""

DATA_SETS = [
    (('impressions', 'affiliate'), BASE_QUERY),
    (('clicks', 'affiliate'), BASE_QUERY),
    (('pinned', 'affiliate'), BASE_QUERY),
    (('blocked', 'affiliate'), BASE_QUERY),
    (('impressions', 'sponsored'), BASE_QUERY),
    (('clicks', 'sponsored'), BASE_QUERY),
    (('pinned', 'sponsored'), BASE_QUERY),
    (('blocked', 'sponsored'), BASE_QUERY),
    (('quality',), QUALITY_QUERY),
]

def _connect(host='localhost', port=None, database=None, user='postgres', password=None):
    connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
    return connection, connection.cursor(cursor_factory=DictCursor)


def _query(cur, query_template, query_args):
    query = query_template % query_args
    cur.execute(query)
    for rec in cur:
        yield rec


def _upload_s3(datafile, key_id, access_key, bucket_name, key):
    with open(datafile) as f:
        md5 = compute_md5(f)

    conn = boto.connect_s3(key_id, access_key)
    bucket = conn.get_bucket(bucket_name, validate=False)

    k = Key(bucket)
    k.key = key

    k.set_contents_from_filename(datafile, md5=md5, replace=True)
    return "s3://%s/%s" % (bucket_name, k.key)


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