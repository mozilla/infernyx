import logging
from collections import namedtuple
import tempfile
import stat
import os
import sys
import boto
import ujson
from boto.s3.key import Key
from boto.utils import compute_md5
from boto.utils import get_instance_metadata
import psycopg2
from psycopg2.extras import DictCursor
import gzip


DataFile = namedtuple('DataFile', ['tempfile', 's3', 'tablename', 'columns'])


def _connect(host='localhost', port=None, database=None, user='postgres', password=None):
    connection = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
    return connection, connection.cursor(cursor_factory=DictCursor)


def _log(jid, msg, severity=logging.INFO):
    logging.log(severity, '%s: %s', jid, msg)


# def _get_columns(kset):
#     keys = kset['key_parts']
#     values = kset['value_parts']
#     return keys[1:] + values
#
#
def _get_columns(keyset):
    # if a column mapping is specified as 'attribute_name':None, then the
    # attribute won't be mapped to the database use this technique to use a
    # key or value for map/reduce, but dispose of it before persisting to db
    if keyset.get('column_mappings', None):
        key_columns = []
        for keyp in keyset['key_parts']:
            if keyp in keyset['column_mappings']:
                if keyset['column_mappings'][keyp]:
                    key_columns.append(keyset['column_mappings'][keyp])
            else:
                key_columns.append(keyp)

        value_columns = []
        for val in keyset['value_parts']:
            if val in keyset['column_mappings']:
                if keyset['column_mappings'][val]:
                    value_columns.append(keyset['column_mappings'][val])
            else:
                value_columns.append(val)
    else:
        key_columns = keyset['key_parts']
        value_columns = keyset['value_parts']

    return key_columns[1:] + value_columns


def _get_sts_credentials():
    metadata = get_instance_metadata()['iam']['security-credentials'].values().pop()
    access_key = metadata['AccessKeyId']
    secret_key = metadata['SecretAccessKey']
    token = metadata['Token']
    credentials = "credentials 'aws_access_key_id=%s;aws_secret_access_key=%s;token=%s'"
    credentials %= (access_key, secret_key, token)
    return credentials


def _build_datafiles(disco_iter, params, job_id):
    pivot = None
    datafiles = []
    columns = ()
    total_lines = 0
    tmp = None

    try:
        for key, value in disco_iter:
            # New keyset was discovered
            if pivot != key[0]:
                pivot = key[0]
                keyset = params.keysets[pivot]
                if tmp:
                    tmp.close()
                tmp_file_name = tempfile.mktemp(prefix=pivot, dir='/tmp')
                tmp = gzip.open(tmp_file_name, 'wb', 1)
                os.chmod(tmp.name, stat.S_IROTH | stat.S_IRGRP | stat.S_IRUSR)
                columns = _get_columns(keyset)
                datafiles.append(DataFile(tmp.name, (None, None), keyset['table'], ','.join(columns)))
                _log(job_id, "Saving %s data in %s" % (keyset['table'], tmp.name))

            data = dict(zip(columns, tuple(key[1:]) + tuple(value)))
            # _log(job_id, 'Debug.persist_results: %s' % escaped, logging.DEBUG)
            tmp.write(ujson.dumps(data) + '\n')
            total_lines += 1

        if tmp:
            tmp.close()
    except Exception as e:
        # Nuke all the data files if any exception has been raised so that no files
        # will be left behind. This is likely to happen when `gzip.open` or `tmp.write`
        # fails upon conditions, e.g. running out of disk space
        _log(job_id, "Failed to build datafiles in the result processor: %s", e)
        for tmpfile, _, _, _ in datafiles:
            _log(job_id, "Cleaning up tmp files: %s" % tmpfile)
            os.unlink(tmpfile)
        raise e

    return datafiles, total_lines


def _insert_datafiles(host, port, database, user, password, datafiles, params, job_id, total_lines, extras=''):
    connection, cursor = _connect(host, port, database, user, password)
    try:
        query = "COPY %s (%s) FROM '%s' WITH %s JSON 'auto' TRUNCATECOLUMNS GZIP"
        for tmpfile, (s3_bucket, s3_key), tablename, columns in datafiles:

            # Default delimiter is |, default escape is backslash
            if s3_bucket and s3_key:
                fle = "s3://%s/%s" % (s3_bucket, s3_key)
            else:
                fle = tmpfile
            command = query % (tablename, columns, fle, extras)
            _log(job_id, "Executing: %s" % command)

            cursor.execute(command)

    except Exception as e:
        _log(job_id, "Error persisting results. Rolling back: %s" % e.message, logging.ERROR)
        import traceback
        trace = traceback.format_exc(15)
        _log(job_id,  trace, logging.ERROR)
        connection.rollback()
        raise e
    else:
        connection.commit()
        _log(job_id, "Processed %d records in %d keysets." % (total_lines, len(params.keysets)))
    finally:
        cursor.close()
        connection.close()
        for tmpfile, s3, _, _ in datafiles:
            try:
                if getattr(params, 'clean_db_files', True):
                    _log(job_id, "Cleaning up tmp files: %s (leaving s3: %s)" % (tmpfile, s3))
                    os.unlink(tmpfile)
            except Exception as e:
                _log(job_id, "Error removing temp file: %s." % e, logging.ERROR)
        sys.stdout.flush()


def _upload_s3(datafiles, job_id, bucket_name='infernyx'):
    rval = []
    for tmpfile, _, tablename, columns in datafiles:
        with open(tmpfile) as f:
            md5 = compute_md5(f)

        conn = boto.connect_s3()
        bucket = conn.get_bucket(bucket_name, validate=False)

        k = Key(bucket)
        k.key = "%s-%s" % (job_id, tmpfile)

        k.set_contents_from_filename(tmpfile, md5=md5, replace=True)

        rval.append(DataFile(tmpfile, (bucket_name, k.key), tablename, columns))
        _log(job_id, "->S3 %s/%s" % (bucket_name, k.key))
    return rval


# this function inserts disco job results to the database
def insert_postgres(disco_iter, params, job_id, host, database, user, password, **kwargs):
    datafiles, total_lines = _build_datafiles(disco_iter, params, job_id)
    _insert_datafiles(host, None, database, user, password, datafiles, params, job_id, total_lines)
    return total_lines


def insert_redshift(disco_iter, params, job_id, host, port, database, user, password, **kwargs):
    datafiles, total_lines = _build_datafiles(disco_iter, params, job_id)
    datafiles = _upload_s3(datafiles, job_id, kwargs.get('bucket_name'))
    credentials = _get_sts_credentials()
    _insert_datafiles(host, port, database, user, password, datafiles, params,
                      job_id, total_lines, extras=credentials)
    return total_lines


# return a list of blacklisted IP addresses
def get_blacklist_ips(host, port, database, user, password):
    connection, cursor = _connect(host, port, database, user, password)
    try:
        query = "select distinct ip from blacklisted_ips"
        cursor.execute(query)
        return set(row['ip'] for row in cursor)
    except:
        return {}
    finally:
        connection.close()


def delete_old_blacklist_ips(host, port, database, user, password):
    connection, cursor = _connect(host, port, database, user, password)
    try:
        query = "delete from blacklisted_ips where date < current_date - 7"
        cursor.execute(query)
    except Exception as e:
        connection.rollback()
        raise e
    else:
        connection.commit()
    finally:
        connection.close()
