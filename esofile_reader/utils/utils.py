def incremental_id_gen():
    """ Incremental id generator. """
    i = 0
    while True:
        i += 1
        yield i
