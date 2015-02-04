from inferno.lib.rule import chunk_json_stream
from inferno.lib.rule import InfernoRule
from disco.func import chain_stream
from functools import partial
from infernyx.utils import kv_reader
import logging

log = logging.getLogger(__name__)

# incoming:site_tuples format
# [u'_default', u'2015-01-08', u'ar', u'DZ', u'accounts.google.com', u'www.elkhabar.com']	[4]
# [u'_default', u'2015-01-08', u'ar', u'DZ', u'add-anime.net', u'topstoreoffers.com']	[6]
# [u'_default', u'2015-01-08', u'ar', u'DZ', u'ar-ar.facebook.com', u'library.islamweb.net']	[12]
# [u'_default', u'2015-01-08', u'ar', u'DZ', u'ar-ar.facebook.com', u'mail.google.com']	[14]
# [u'_default', u'2015-01-08', u'ar', u'DZ', u'ar-ar.facebook.com', u'wiki.mozilla.org']	[10]
# [u'_default', u'2015-01-08', u'ar', u'DZ', u'ar-ar.facebook.com', u'www.facebook.com']	[27]
# [u'_default', u'2015-01-08', u'ar', u'DZ', u'flash-games.jsoftj.com', u'www.flickr.com']	[4]
# [u'_default', u'2015-01-08', u'ar', u'DZ', u'l.facebook.com', u'www.mozilla.org']	[3]
# [u'_default', u'2015-01-08', u'ar', u'DZ', u'mail.google.com', u'secure.skype.com']	[28]
# [u'_default', u'2015-01-08', u'ar', u'DZ', u'www.facebook.com', u'www.startimes.com']	[1]


def filter_site(parts, params):
    filter_for_site = params.filter_for_site
    if parts['url_a'] == filter_for_site or parts['url_b'] == filter_for_site:
        yield parts

RULES = [
    InfernoRule(
        name='analyze_tuples',

        # from the command line - override the input tags with the "-t" option
        source_tags=['incoming:site_tuples'],

        map_input_stream=chain_stream + (partial(kv_reader,
                                                 keys=('keyset', 'date', 'locale', 'country_code', 'url_a', 'url_b'),
                                                 values=('count',)),),

        key_parts=['date'],
        value_parts=['count'],

        parts_preprocess=[filter_site],

        # override this on the command line with:
        #   -P 'filter_for_site: override.org'
        filter_for_site='booking.com',

        partitions=32,
        sort_buffer_size='35%',
    ),
]
