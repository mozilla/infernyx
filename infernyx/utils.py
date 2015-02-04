from disco.func import chain_stream


def kv_reader(stream, size=None, url=None, params=None, keys=(), values=()):
    for ins in stream:
        ks, vs = ins
        kval = dict(zip(keys, ks))
        vval = dict(zip(values, vs))
        kval.update(vval)
        yield kval


chunk_kv_stream = (chain_stream, kv_reader)