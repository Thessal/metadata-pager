import hashlib


def calc_self_hash(_file, _locals, _varnames):
    # _locals = locals()
    # _varnames = self.__init__.__code__.co_varnames
    _varnames = [x for x in _varnames if x != 'self']
    _vars = {x: _locals[x] for x in _varnames if (x in _locals)}
    _mutable_types = [str, int, bool, float, ]
    assert (all((type(x) in _mutable_types) for x in _vars.keys()))
    with open(_file, 'rb') as f:
        hash_ = hashlib.md5(f.read()).digest()
    hash_ += hashlib.md5(str(_vars).encode()).digest()
    hash_ = hashlib.md5(hash_).hexdigest()
    print(hash_)
    return hash_
