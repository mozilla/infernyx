from inferno.lib.rule import chunk_json_stream
from inferno.lib.rule import InfernoRule
from infernyx.rules import combiner

AUTO_RUN = False


def count(parts, params):
    parts['count'] = 1
    yield parts

RULES = [
    InfernoRule(
        name='busiest_ips',
        source_tags=['processed:impression'],
        day_range=1,
        map_input_stream=chunk_json_stream,
        parts_preprocess=[count],
        partitions=32,
        sort_buffer_size='25%',
        combiner_function=combiner,
        key_parts=['ip'],
        value_parts=['count'],
    ),
]



