def incremental_id_gen():
    """ Incremental id generator. """
    i = 0
    while True:
        i += 1
        yield i


def slice_dict(dct, keys):
    """ Slice dictionary using given keys. """
    return {key: dct[key] for key in keys if key in dct}
