from functools import wraps
from typing import Callable


def repr_str(obj):
    if isinstance(obj, str):
        return obj
    return repr(obj)


def print_result(func):
    @wraps(func)
    def wrapper(*args, **kw):
        result = func(*args, **kw)
        print(f"{args[0].obj__}, {args[0].expr__}")
        print(f"{result=}")
        return result
    return wrapper


def get_name(obj, otherwise: Callable = repr_str):
    if isinstance(obj, str):
        return obj
    for attr in ("__qualname__", "__name__", "name"):
        if hasattr(obj, attr):
            return getattr(obj, attr)
    return otherwise(obj)
