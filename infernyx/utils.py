from disco.func import chain_stream


def kv_reader(stream, size=None, url=None, params=None, keyset=None, keys=(), values=()):
    for ks, vs in stream:
        if keyset is None or (len(ks) and ks[0] == keyset):
            kval = dict(zip(keys, ks))
            vval = dict(zip(values, vs))
            kval.update(vval)
            yield kval


chunk_kv_stream = chain_stream + (kv_reader,)