"""Dask object core module."""

from collections import Counter
from operator import attrgetter, itemgetter, methodcaller

import dask.bag as db
import toolz
from dask import compute, persist
from dask.delayed import Delayed, delayed
from dask.distributed import as_completed, get_client


def summer(value, *args):
    for _ in args:
        value = value + _
    return value


def counter(items):
    return Counter(items).items()


def sum_counts(values):
    value, *args = values
    value = list(value)
    if len(value) == 1:
        value = value[0]
    value = Counter(dict(value))
    for _ in args:
        value.update(dict(_))
    return value


class DaskObjects:
    """Dask distributed objects.

    Dask object provides a simple interface to create and manage collections of objects,
    potentially distributed across multiple workers.

    Dask object provides attribute access to the underlying collection of objects,
    mapping the attribute access to the underlying collection of objects.
    """
    items: db.Bag

    def __init__(self, items, config=None, use_bag_attrs=False, **kwargs):
        self.config = config or {}
        self.use_bag_attrs = use_bag_attrs or self.config.get("use_bag_attrs", False)
        self.config["use_bag_attrs"] = use_bag_attrs
        self.config.update(kwargs)
        if isinstance(items, db.Bag):
            npartitions = kwargs.pop('npartitions', None)
            if npartitions is not None and items.npartitions != npartitions:
                items = items.repartition(npartitions)
            self.items = items
        else:
            npartitions = kwargs.pop('npartitions', len(items))
            self.items = db.from_sequence(items, npartitions=npartitions, **kwargs)

    def _make_new(self, items):
        return type(self)(items, **self.config)

    def __getattr__(self, attr):
        func = attrgetter(attr)
        if self.use_bag_attrs and hasattr(self.items, attr):
            return func(self.items)
        return self._make_new(self.items.map(func))

    def __getitem__(self, item):
        return self._make_new(self.items.pluck(item))

    def __call__(self, *args, **kwargs):
        return self._make_new(self.items.map(lambda f: f(*args, **kwargs)))

    def call(self, method, *args, **kwargs):
        func = methodcaller(method, *args, **kwargs)
        return self._make_new(self.items.map(func))

    def map(self, func, *args, compute=False, **kwargs):
        out = self._make_new(self.items.map(func, *args, **kwargs))
        if compute:
            return out.compute()
        return out

    def compute(self, *args, flatten=False, **kwargs):
        items = self.items
        if flatten:
            items = items.flatten()
        return items.compute(*args, **kwargs)

    def persist(self, *args, **kwargs):
        return self._make_new(self.items.persist(*args, **kwargs))

    def flatten(self, compute=False):
        if compute:
            return self.compute(flatten=True)
        return self._make_new(self.items.flatten())

    def __repr__(self):
        return f"{type(self)}({repr(self.items)})"

    def __str__(self):
        return f"{type(self)}({str(self.items)})"

    def reduction(self, *args, **kwargs):
        return self._make_new(self.items.reduction(*args, **kwargs))

    def counts(self, split_every=None):
        return self.items.reduction(counter, sum_counts, split_every=split_every).compute()


# @delayed
def noop(arg):
    return arg


class DaskDelayedObjects:
    def __init__(self, items, **kwargs) -> None:
        self.kwargs = kwargs
        first, items = toolz.peek(items)
        _noop = noop
        if not isinstance(first, Delayed):
            _noop = delayed(noop)
        self.items = list(map(_noop, items))
        try:
            client = get_client()
        except ValueError:
            client = None
        self.client = client

    @property
    def _map(self):
        if self.client is None:
            return map
        return self.client.map

    def _make_new(self, items):
        return type(self)(items, **self.kwargs)

    def map(self, func, *args, compute=False, **kwargs):
        f = toolz.curry(delayed(func), *args, **kwargs)
        out = self._make_new(self._map(f, self.items))
        if compute:
            out = out.compute()
        return out

    def compute(self, *args, **kwargs):
        out = compute(self.items, *args, **kwargs)
        if len(out) == 1:
            out = out[0]
        return out

    def persist(self, *args, **kwargs):
        return self._make_new(persist(self.items, *args, **kwargs))

    def __getattr__(self, attr):
        return self.map(attrgetter(attr))

    def __getitem__(self, item):
        return self.map(itemgetter(item))

    def __call__(self, *args, **kwargs):
        return self.map(lambda f: f(*args, **kwargs))

    def call(self, method, *args, **kwargs):
        return self.map(methodcaller(method, *args, **kwargs))

    def __iter__(self):
        for _ in as_completed(self.items):
            yield _.result()

    def __repr__(self):
        return f"{type(self)}({repr(self.items)})"

    def __str__(self):
        return f"{type(self)}({str(self.items)})"

    def __len__(self):
        return len(self.items)

    @staticmethod
    def make_op(name):
        def func(self, *args, **kwargs):
            return self.map(methodcaller(name, *args, **kwargs))
        func.__name__ = name
        return func

    __add__ = make_op("__add__")
    __sub__ = make_op("__sub__")
    __mul__ = make_op("__mul__")
    __truediv__ = make_op("__truediv__")
    __floordiv__ = make_op("__floordiv__")
    __mod__ = make_op("__mod__")
    __pow__ = make_op("__pow__")
    __lshift__ = make_op("__lshift__")
    __rshift__ = make_op("__rshift__")
    __and__ = make_op("__and__")
    __xor__ = make_op("__xor__")
    __or__ = make_op("__or__")
    __lt__ = make_op("__lt__")
    __le__ = make_op("__le__")
    __eq__ = make_op("__eq__")
    __ne__ = make_op("__ne__")
    __gt__ = make_op("__gt__")
    __ge__ = make_op("__ge__")
    __radd__ = make_op("__radd__")
    __rsub__ = make_op("__rsub__")
    __rmul__ = make_op("__rmul__")
    __rtruediv__ = make_op("__rtruediv__")
    __rfloordiv__ = make_op("__rfloordiv__")
    __rmod__ = make_op("__rmod__")
    __rpow__ = make_op("__rpow__")
    __rlshift__ = make_op("__rlshift__")
    __rrshift__ = make_op("__rrshift__")
    __rand__ = make_op("__rand__")
    __rxor__ = make_op("__rxor__")
    __ror__ = make_op("__ror__")
    __neg__ = make_op("__neg__")
    __pos__ = make_op("__pos__")
    __abs__ = make_op("__abs__")
    __invert__ = make_op("__invert__")
