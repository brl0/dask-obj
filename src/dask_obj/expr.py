"""Expression module."""

from operator import methodcaller
from typing import Any


def op(name):
    def func(self, *args, **kwargs):
        return self._make_new(name, *args, **kwargs)
    func.__name__ = name
    return func


class Expr:
    """Expression base class.

    Expression base class will be used to record method calls and their arguments
    in order to be able to replay them later on.

    """

    def __init__(self, name, obj, *args, expr=None, **kwargs):
        self.name = name
        if isinstance(obj, type):
            typ = obj
        else:
            if not expr and (args or kwargs):
                raise ValueError(f"Cannot set args or kwargs for {obj}")
            typ = type(obj)
            self._obj = obj
        self.typ = typ
        self.args = args
        self.kwargs = kwargs
        self.expr = expr

    @property
    def has_obj(self):
        return hasattr(self, "_obj")

    @property
    def obj(self):
        """Return the object."""
        if not self.has_obj:
            args = object.__getattribute__(self, "args")
            kwargs = object.__getattribute__(self, "kwargs")
            self._obj = self.typ(*args, **kwargs)
        return self._obj

    @obj.setter
    def obj(self, obj):
        """Set the object."""
        self._obj = obj

    @obj.deleter
    def obj(self):
        """Delete the object."""
        del self._obj

    def __getattribute__(self, name: str) -> Any:
        if hasattr(type(self), name) or hasattr(object, name) or \
            (name.startswith("__") and not name.endswith("__")) or \
                name in {"expr", "typ", "__hash__", "_obj", "obj"} or \
                any(_ in name for _ in {"repr", "mro", "getattr"}):
            return object.__getattribute__(self, name)

        if self.has_obj:
            arg = object.__getattribute__(self, "obj")
        else:
            arg = object.__getattribute__(self, "typ")

        if hasattr(arg, name):
            return type(self)(name, arg, expr=self)

        typ = object.__getattribute__(self, "typ")
        raise AttributeError(f"{typ.__name__} has no attribute {name}")

    def _make_new(self, name, *args, expr=None, **kwargs):
        if expr is None:
            expr = self
        if self.has_obj:
            arg = self.obj
        else:
            arg = self.typ
        return type(self)(name, arg, *args, expr=expr, **kwargs)

    def __call__(self, *args, **kwargs):
        name = object.__getattribute__(self, "name")
        return self._make_new(name, *args, expr=self.expr, **kwargs)

    def _args_str(self):
        args = object.__getattribute__(self, "args")
        if args:
            args = ", ".join([f"'{a}'" if isinstance(a, str) else str(a) for a in args])
        else:
            args = ""
        kwargs = object.__getattribute__(self, "kwargs")
        if kwargs:
            kwargs = ", ".join(f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}" for k, v in kwargs.items())
        else:
            kwargs = ""
        args = ", ".join(filter(None, [args, kwargs]))
        return args

    def __str__(self) -> str:
        name = object.__getattribute__(self, "name")
        typ = object.__getattribute__(self, "typ")
        expr = object.__getattribute__(self, "expr")
        args = self._args_str()
        if expr is None:
            if not self.has_obj:
                return f"{name} = {typ.__name__}({args})"
            return f"{name} = ({self.obj})"
        out = f"{expr}.{name}"
        if callable(getattr(typ, name)):
            out += f"({args})"
        return out

    def __repr__(self) -> str:
        name = object.__getattribute__(self, "name")
        typ = object.__getattribute__(self, "typ")
        expr = object.__getattribute__(self, "expr")
        args = self._args_str()
        args = [f"'{name}'", typ.__name__, args]
        if expr is not None:
            args.append(f"expr={repr(expr)}")
        args = filter(None, args)
        return f"{type(self).__name__}({', '.join(args)})"

    def __hash__(self) -> int:
        return hash(repr(self))

    def eval(self, value=None):
        """Evaluate the expression."""
        expr = object.__getattribute__(self, "expr")
        if expr is None:
            if value is None:
                return self.obj
            return value
        name = object.__getattribute__(self, "name")
        args = object.__getattribute__(self, "args")
        kwargs = object.__getattribute__(self, "kwargs")
        caller = methodcaller(name, *args, **kwargs)
        return caller(expr.eval(value))

    __add__ = op("__add__")
    __sub__ = op("__sub__")
    __mul__ = op("__mul__")
    __truediv__ = op("__truediv__")
    __floordiv__ = op("__floordiv__")
    __mod__ = op("__mod__")
    __pow__ = op("__pow__")
    __lshift__ = op("__lshift__")
    __rshift__ = op("__rshift__")
    __and__ = op("__and__")
    __xor__ = op("__xor__")
    __or__ = op("__or__")
    __lt__ = op("__lt__")
    __le__ = op("__le__")
    __eq__ = op("__eq__")
    __ne__ = op("__ne__")
    __gt__ = op("__gt__")
    __ge__ = op("__ge__")
    __radd__ = op("__radd__")
    __rsub__ = op("__rsub__")
    __rmul__ = op("__rmul__")
    __rtruediv__ = op("__rtruediv__")
    __rfloordiv__ = op("__rfloordiv__")
    __rmod__ = op("__rmod__")
    __rpow__ = op("__rpow__")
    __rlshift__ = op("__rlshift__")
    __rrshift__ = op("__rrshift__")
    __rand__ = op("__rand__")
    __rxor__ = op("__rxor__")
    __ror__ = op("__ror__")
    __neg__ = op("__neg__")
    __pos__ = op("__pos__")
    __abs__ = op("__abs__")
    __invert__ = op("__invert__")
