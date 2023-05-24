"""Dask object core module."""

from collections import Counter
from operator import attrgetter, itemgetter, methodcaller

import dask.bag as db
from dask import compute, persist
from dask.delayed import delayed
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


@delayed
def noop(arg):
    return arg


class DaskDelayedObjects:
    def __init__(self, items, **kwargs) -> None:
        self.kwargs = kwargs
        self.items = list(map(noop, items))
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
        return compute(self.items, *args, **kwargs)

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
