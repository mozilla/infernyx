from functools import partial
from boto.s3.connection import S3Connection
import re
import disco.schemes.scheme_raw
from disco.worker.task_io import task_input_stream
from inferno.lib.rule import InfernoRule

AUTORUN = False


def init(input_iter, params):
    import disco.ddfs
    params.ddfs = disco.ddfs.DDFS("disco://%s" % params.server)


def s3_import_map(line, params):
    import tempfile
    from boto.s3.connection import S3Connection
    import os

    # print ("Processing line %s" % line)
    tag, bucket_name, key_name = line.split('/')
    conn = S3Connection()
    bucket = conn.get_bucket(bucket_name)
    key = bucket.get_key(key_name)
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        key.get_contents_to_file(tmpfile)
    except Exception as e:
        print ('Adding Failed: %s' % line)
        yield ('failed', line), [1]
    else:
        tmpfile.close()
        tags, blobs = params.ddfs.chunk(tag, [tmpfile.name])
        # print "Blobs created: %s %s" % (tags, blobs)
        os.unlink(tmpfile.name)
        yield ('OK', ), [1]


def get_keys_for_pattern(bucket, pattern, tag_expr, prefix=''):

    print "-->", bucket, pattern, prefix
    conn = S3Connection()
    bucket = conn.get_bucket(bucket)
    urls = []
    reg_exp = re.compile(pattern)

    for key in bucket.list(prefix=prefix):
        match = reg_exp.search(key.name)
        if match:
            tag = ''
            for element in tag_expr:
                if isinstance(element, str):
                    tag += element
                else:
                    # assume int - which is a group from the expression
                    tag += match.group(element)
            raw_url = "raw://%s/%s/%s" % (tag, bucket.name, key.name)
            # print 'match: ' + raw_url
            urls.append(raw_url)
    conn.close()
    return urls


def filename_input_stream(fd, size, url, params):
    """This input_stream simply returns the path of the local disk file for this map job"""
    from disco import util
    from disco.worker.classic import worker

    try:
        scheme, netloc, rest = util.urlsplit(url)
    except Exception as e:
        msg = "Error handling hustle_input_stream for %s. %s" % (url, e)
        raise util.DataError(msg, url)

    if scheme == 'file':
        yield netloc, '/' + rest
    else:
        # print url, rest
        fle = util.localize(rest,
                            disco_data=worker.Task.disco_data,
                            ddfs_data=worker.Task.ddfs_data)

        yield netloc, fle


def copy_tags_map(intup, params):
    from disco.ddfs import DDFS
    from socket import gethostname
    netloc, local_file = intup
    try:
        ddfs = DDFS(params.target_disco_master)
        if params.chunk:
            ddfs.chunk(params.target_tag, [local_file])
        else:
            ddfs.push(params.target_tag, [local_file])
        # print local_file
    except Exception as e:
        yield '["_default", "%s", "%s", "%s"]' % (gethostname(), netloc, local_file)


RULES = [
    # this rule loads data into a cluster from s3
    InfernoRule(
        name='bulk_load',
        source_urls=partial(get_keys_for_pattern,
                            bucket='tiles-incoming-prod-us-west-2',
                            pattern=r'.+-([^-]*)-(2015\.01\.(05|06|07|08|09|10|11|12|13))',
                            tag_expr=["processed:", 1, ":2015-01-", 3]),
        map_input_stream=(disco.schemes.scheme_raw.input_stream,),
        map_init_function=init,
        map_function=s3_import_map,
    ),
    # this rule copies tags from one Disco cluster to another
    InfernoRule(
        name='copy_tags',
        source_tags=[],
        target_disco_master='disco://localhost',
        target_tag='',
        chunk=False,
        map_input_stream=(task_input_stream, filename_input_stream),
        map_function=copy_tags_map,
    ),
]


