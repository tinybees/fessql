#!/usr/bin/env python3
# coding=utf-8

"""
@author: guoyanfeng
@software: PyCharm
@time: 2020/3/1 下午7:30

  * :class:`LRI` - Least-recently inserted
  * :class:`LRU` - Least-recently used

  * ``hit_count`` - the number of times the queried key has been in
    the cache
  * ``miss_count`` - the number of times a key has been absent and/or
    fetched by the cache
  * ``soft_miss_count`` - the number of times a key has been absent,
    but a default has been provided by the caller, as with
    :meth:`dict.get` and :meth:`dict.setdefault`. Soft misses are a
    subset of misses, so this number is always less than or equal to
    ``miss_count``.

由boltons库的cacheutils改造
"""

try:
    from threading import RLock
except ImportError:
    class RLock(object):
        """Dummy reentrant lock for builds without threads"""

        def __enter__(self):
            pass

        def __exit__(self, exctype, excinst, exctb):
            pass

try:
    # noinspection PyUnresolvedReferences
    from boltons.typeutils import make_sentinel

    _MISSING = make_sentinel(var_name='_MISSING')
    _KWARG_MARK = make_sentinel(var_name='_KWARG_MARK')
except ImportError:
    _MISSING = object()
    _KWARG_MARK = object()

__all__ = ("LRI", "LRU")

PREV, NEXT, KEY, VALUE = range(4)  # names for the link fields
DEFAULT_MAX_SIZE = 128


class LRI(dict):
    """The ``LRI`` implements the basic *Least Recently Inserted* strategy to
    caching. One could also think of this as a ``SizeLimitedDefaultDict``.

    *on_miss* is a callable that accepts the missing key (as opposed
    to :class:`collections.defaultdict`'s "default_factory", which
    accepts no arguments.) Also note that, like the :class:`LRI`,
    the ``LRI`` is instrumented with statistics tracking.

    >>> cap_cache = LRI(max_size=2)
    >>> cap_cache['a'], cap_cache['b'] = 'A', 'B'
    >>> from pprint import pprint as pp
    >>> pp(dict(cap_cache))
    {'a': 'A', 'b': 'B'}
    >>> [cap_cache['b'] for i in range(3)][0]
    'B'
    >>> cap_cache['c'] = 'C'
    >>> print(cap_cache.get('a'))
    None
    >>> cap_cache.hit_count, cap_cache.miss_count, cap_cache.soft_miss_count
    (3, 1, 1)
    """

    def __init__(self, max_size=DEFAULT_MAX_SIZE, values=None, on_miss=None):
        super().__init__()
        if max_size <= 0:
            raise ValueError('expected max_size > 0, not %r' % max_size)
        self.hit_count = self.miss_count = self.soft_miss_count = 0
        self.max_size = max_size
        self._lock = RLock()
        self._init_ll()

        if on_miss is not None and not callable(on_miss):
            raise TypeError('expected on_miss to be a callable'
                            ' (or None), not %r' % on_miss)
        self.on_miss = on_miss

        if values:
            self.update(values)

    # invariants:
    # 1) 'anchor' is the sentinel node in the doubly linked list.  there is
    #    always only one, and its KEY and VALUE are both _MISSING.
    # 2) the most recently accessed node comes immediately before 'anchor'.
    # 3) the least recently accessed node comes immediately after 'anchor'.
    def _init_ll(self):
        anchor = []
        anchor[:] = [anchor, anchor, _MISSING, _MISSING]
        # a link lookup table for finding linked list links in O(1)
        # time.
        self._link_lookup = {}
        self._anchor = anchor

    def _get_flattened_ll(self):
        flattened_list = []
        link = self._anchor
        while True:
            flattened_list.append((link[KEY], link[VALUE]))
            link = link[NEXT]
            if link is self._anchor:
                break
        return flattened_list

    def _get_link_and_move_to_front_of_ll(self, key):
        # find what will become the newest link. this may raise a
        # KeyError, which is useful to __getitem__ and __setitem__
        newest = self._link_lookup[key]

        # splice out what will become the newest link.
        newest[PREV][NEXT] = newest[NEXT]
        newest[NEXT][PREV] = newest[PREV]

        # move what will become the newest link immediately before
        # anchor (invariant 2)
        anchor = self._anchor
        second_newest = anchor[PREV]
        second_newest[NEXT] = anchor[PREV] = newest
        newest[PREV] = second_newest
        newest[NEXT] = anchor
        return newest

    def _set_key_and_add_to_front_of_ll(self, key, value):
        # create a new link and place it immediately before anchor
        # (invariant 2).
        anchor = self._anchor
        second_newest = anchor[PREV]
        newest = [second_newest, anchor, key, value]
        second_newest[NEXT] = anchor[PREV] = newest
        self._link_lookup[key] = newest

    def _set_key_and_evict_last_in_ll(self, key, value):
        # the link after anchor is the oldest in the linked list
        # (invariant 3).  the current anchor becomes a link that holds
        # the newest key, and the oldest link becomes the new anchor
        # (invariant 1).  now the newest link comes before anchor
        # (invariant 2).  no links are moved; only their keys
        # and values are changed.
        oldanchor = self._anchor
        oldanchor[KEY] = key
        oldanchor[VALUE] = value

        self._anchor = anchor = oldanchor[NEXT]
        evicted = anchor[KEY]
        anchor[KEY] = anchor[VALUE] = _MISSING
        del self._link_lookup[evicted]
        self._link_lookup[key] = oldanchor
        return evicted

    def _remove_from_ll(self, key):
        # splice a link out of the list and drop it from our lookup
        # table.
        link = self._link_lookup.pop(key)
        link[PREV][NEXT] = link[NEXT]
        link[NEXT][PREV] = link[PREV]

    def __setitem__(self, key, value):
        with self._lock:
            try:
                link = self._get_link_and_move_to_front_of_ll(key)
            except KeyError:
                if len(self) < self.max_size:
                    self._set_key_and_add_to_front_of_ll(key, value)
                else:
                    evicted = self._set_key_and_evict_last_in_ll(key, value)
                    super(LRI, self).__delitem__(evicted)
                super(LRI, self).__setitem__(key, value)
            else:
                link[VALUE] = value

    def __getitem__(self, key):
        with self._lock:
            try:
                link = self._link_lookup[key]
            except KeyError:
                self.miss_count += 1
                if not self.on_miss:
                    raise
                ret = self[key] = self.on_miss(key)
                return ret

            self.hit_count += 1
            return link[VALUE]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self.soft_miss_count += 1
            return default

    def __delitem__(self, key):
        with self._lock:
            super(LRI, self).__delitem__(key)
            self._remove_from_ll(key)

    def pop(self, key, default=_MISSING):
        # NB: hit/miss counts are bypassed for pop()
        with self._lock:
            try:
                ret = super(LRI, self).pop(key)
            except KeyError:
                if default is _MISSING:
                    raise
                ret = default
            else:
                self._remove_from_ll(key)
            return ret

    def popitem(self):
        with self._lock:
            item = super(LRI, self).popitem()
            self._remove_from_ll(item[0])
            return item

    def clear(self):
        with self._lock:
            super(LRI, self).clear()
            self._init_ll()

    def copy(self):
        return self.__class__(max_size=self.max_size, values=self)

    def setdefault(self, key, default=None):
        with self._lock:
            try:
                return self[key]
            except KeyError:
                self.soft_miss_count += 1
                self[key] = default
                return default

    def update(self, E, **F):
        # E and F are throwback names to the dict() __doc__
        with self._lock:
            if E is self:
                return
            setitem = self.__setitem__
            if callable(getattr(E, 'keys', None)):
                for k in E.keys():
                    setitem(k, E[k])
            else:
                for k, v in E:
                    setitem(k, v)
            for k in F:
                setitem(k, F[k])
            return

    def __eq__(self, other):
        with self._lock:
            if self is other:
                return True
            if len(other) != len(self):
                return False
            if not isinstance(other, LRI):
                return other == self
            return super(LRI, self).__eq__(other)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        cn = self.__class__.__name__
        val_map = super(LRI, self).__repr__()
        return ('%s(max_size=%r, on_miss=%r, values=%s)'
                % (cn, self.max_size, self.on_miss, val_map))


# noinspection PyUnresolvedReferences
class LRU(LRI):
    """The ``LRU`` is :class:`dict` subtype implementation of the
    *Least-Recently Used* caching strategy.

    Args:
        max_size (int): Max number of items to cache. Defaults to ``128``.
        values (iterable): Initial values for the cache. Defaults to ``None``.
        on_miss (callable): a callable which accepts a single argument, the
            key not present in the cache, and returns the value to be cached.

    >>> cap_cache = LRU(max_size=2)
    >>> cap_cache['a'], cap_cache['b'] = 'A', 'B'
    >>> from pprint import pprint as pp
    >>> pp(dict(cap_cache))
    {'a': 'A', 'b': 'B'}
    >>> [cap_cache['b'] for i in range(3)][0]
    'B'
    >>> cap_cache['c'] = 'C'
    >>> print(cap_cache.get('a'))
    None

    This cache is also instrumented with statistics
    collection. ``hit_count``, ``miss_count``, and ``soft_miss_count``
    are all integer members that can be used to introspect the
    performance of the cache. ("Soft" misses are misses that did not
    raise :exc:`KeyError`, e.g., ``LRU.get()`` or ``on_miss`` was used to
    cache a default.

    >>> cap_cache.hit_count, cap_cache.miss_count, cap_cache.soft_miss_count
    (3, 1, 1)

    Other than the size-limiting caching behavior and statistics,
    ``LRU`` acts like its parent class, the built-in Python :class:`dict`.
    """

    def __getitem__(self, key):
        with self._lock:
            try:
                link = self._get_link_and_move_to_front_of_ll(key)
            except KeyError:
                self.miss_count += 1
                if not self.on_miss:
                    raise
                ret = self[key] = self.on_miss(key)
                return ret

            self.hit_count += 1
            return link[VALUE]
