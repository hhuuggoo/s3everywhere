class CachedProperty(object):
    def __init__(self, func):
        self.func = func

    def __get__(self, obj, cls):
        if obj is None: return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value

cached_property = CachedProperty


def to_bytes(x):
    if isinstance(x, str):
        return x.encode('utf-8', 'replace')
    return x
