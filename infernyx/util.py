
def csv_output_stream(stream, partition, url, params):
    from disco.fileutils import DiscoOutputStream_v1
    import csv

    class CsvOutputStream(DiscoOutputStream_v1):
        def __init__(self, stream, params, **kwargs):
            super(CsvOutputStream, self).__init__(stream, **kwargs)
            self.params = params
            self.writer = csv.writer()

        def add(self, k, v):
            # just convert key and value tuples to a dict, then append
            # note we need to use the _keyset to determine how to build the dicts
            keyset = self.params.keysets[k[0]]
            record = dict(zip(keyset['key_parts'], k) + zip(keyset['value_parts'], v))
            self.append(ujson.dumps(record))

    return JsonOutputStream(stream, params)


