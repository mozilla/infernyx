import logging
from collections import namedtuple
import tempfile
import stat
import os
import sys
import subprocess
import boto
import ujson
from boto.s3.key import Key
from boto.utils import compute_md5
from boto.utils import get_instance_metadata
import psycopg2
from psycopg2.extras import DictCursor


RAW_FILE_MAX_LINE = 1500000

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
    keyset_lines = 0
    tmp = None
    keyset_tmp_file_prefix = None
    keyset_tmp_file_list = None

    try:
        for key, value in disco_iter:
            # New keyset was discovered
            if pivot != key[0]:
                pivot = key[0]
                keyset = params.keysets[pivot]
                if tmp:
                    tmp.close()
                keyset_tmp_file_prefix = tempfile.mktemp(prefix=pivot, dir='/tmp')
                tmp = open("%s.00" % keyset_tmp_file_prefix, "wb")
                os.chmod(tmp.name, stat.S_IROTH | stat.S_IRGRP | stat.S_IRUSR)
                columns = _get_columns(keyset)
                keyset_tmp_file_list = [tmp.name]
                datafiles.append(DataFile(keyset_tmp_file_list, (None, None), keyset['table'], ','.join(columns)))
                keyset_lines = 0
                _log(job_id, "Saving %s data in %s" % (keyset['table'], tmp.name))

            # Create a new file if it reached the max line
            if keyset_lines > RAW_FILE_MAX_LINE:
                tmp.close()
                tmp = open("%s.%02d" % (keyset_tmp_file_prefix, len(keyset_tmp_file_list)), "wb")
                os.chmod(tmp.name, stat.S_IROTH | stat.S_IRGRP | stat.S_IRUSR)
                keyset_tmp_file_list.append(tmp.name)
                keyset_lines = 0
                _log(job_id, "Saving %s data in %s" % (keyset['table'], tmp.name))

            data = dict(zip(columns, tuple(key[1:]) + tuple(value)))
            tmp.write(ujson.dumps(data) + '\n')
            total_lines += 1
            keyset_lines += 1

        if tmp:
            tmp.close()
    except Exception as e:
        # Nuke all the data files if any exception has been raised so that no files
        # will be left behind. This is likely to happen when `tmp.write` fails upon
        # e.g. running out of disk space
        _log(job_id, "Failed to build datafiles in the result processor: %s", e)
        for tmp_file_list, _, _, _ in datafiles:
            for tmp_file in tmp_file_list:
                _log(job_id, "Cleaning up tmp file: %s" % tmp_file)
                os.unlink(tmp_file)
        raise e

    return datafiles, total_lines


def _insert_datafiles(host, port, database, user, password, datafiles, params, job_id, total_lines, extras=''):
    connection, cursor = _connect(host, port, database, user, password)
    try:
        query = "COPY %s (%s) FROM '%s' WITH %s JSON 'auto' TRUNCATECOLUMNS GZIP manifest"
        for tmp_file_list, (s3_bucket, s3_key), tablename, columns in datafiles:
            fle = "s3://%s/%s" % (s3_bucket, s3_key)
            command = query % (tablename, columns, fle, extras)
            _log(job_id, "Executing: %s" % command)
            cursor.execute(command)
    except Exception as e:
        _log(job_id, "Error persisting results. Rolling back: %s" % e.message, logging.ERROR)
        import traceback
        trace = traceback.format_exc(15)
        _log(job_id, trace, logging.ERROR)
        connection.rollback()
        raise e
    else:
        connection.commit()
        _log(job_id, "Processed %d records in %d keysets." % (total_lines, len(params.keysets)))
    finally:
        cursor.close()
        connection.close()
        for tmp_file_list, s3, _, _ in datafiles:
            for tmpfile in tmp_file_list:
                try:
                    if getattr(params, 'clean_db_files', True):
                        _log(job_id, "Cleaning up tmp files: %s (leaving s3: %s)" % (tmpfile, s3))
                        os.unlink(tmpfile)
                except Exception as e:
                    _log(job_id, "Error removing temp file: %s." % e, logging.ERROR)
        sys.stdout.flush()


def _compress_datafiles(datafiles, job_id):
    rval = []
    try:
        for tmp_file_list, _, _, _ in datafiles:
            prefix = tmp_file_list[0].rsplit('.')[0]
            # We can further tune the parallel level with `xargs -P`
            _log(job_id, "Compressing datafiles : %s" % tmp_file_list)
            cmd = "ls %s.* | xargs gzip -1" % prefix
            subprocess.check_call(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        # Nuke all the data files if any exception has been raised so that no files
        # will be left behind
        _log(job_id, "Failed to compress datafiles in the result processor: %s", e)
        for tmp_file_list, _, _, _ in datafiles:
            for tmp_file in tmp_file_list:
                _log(job_id, "Cleaning up tmp&gzip file for %s" % tmp_file)
                try:
                    os.unlink(tmp_file)
                except:
                    pass
                try:
                    os.unlink(tmp_file + ".gz")
                except:
                    pass
        raise e
    else:
        # Update the name of the tmp files
        for tmp_file_list, s3, tablename, columns in datafiles:
            new_list = map(lambda v: v + ".gz", tmp_file_list)
            rval.append(DataFile(new_list, s3, tablename, columns))

    return rval


def _upload_s3(datafiles, job_id, bucket_name='infernyx'):
    rval = []

    conn = boto.connect_s3()
    bucket = conn.get_bucket(bucket_name, validate=False)

    for tmp_file_list, _, tablename, columns in datafiles:
        s3_entries = []
        for tmpfile in tmp_file_list:
            with open(tmpfile) as f:
                md5 = compute_md5(f)

            k = Key(bucket)
            k.key = "%s-%s" % (job_id, tmpfile)

            _log(job_id, "->S3 %s/%s" % (bucket_name, k.key))
            k.set_contents_from_filename(tmpfile, md5=md5, replace=True)

            s3_entry = {"url": "s3://%s/%s" % (bucket_name, k.key), "mandatory": True}
            s3_entries.append(s3_entry)

        # upload the manifest
        prefix = tmp_file_list[0].rsplit('.')[0]
        manifest = ujson.dumps({"entries": s3_entries})
        manifest_key = Key(bucket)
        manifest_key.key = "%s.%s.manifest" % (job_id, prefix)
        _log(job_id, "->S3 %s/%s: %s" % (bucket_name, manifest_key.key, manifest))
        manifest_key.set_contents_from_string(manifest)

        # store manifest
        rval.append(DataFile(tmp_file_list, (bucket_name, manifest_key.key), tablename, columns))

    return rval


def insert_redshift(disco_iter, params, job_id, host, port, database, user, password, **kwargs):
    datafiles, total_lines = _build_datafiles(disco_iter, params, job_id)
    datafiles = _compress_datafiles(datafiles, job_id)
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
