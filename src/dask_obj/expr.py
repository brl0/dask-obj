"""Expression module."""

import logging
from operator import methodcaller
from typing import Any, Callable

import toolz
from boltons.funcutils import format_invocation
from boltons.typeutils import make_sentinel
from toolz import flip


_getattr = object.__getattribute__
NO_VALUE = make_sentinel("NO_VALUE", var_name="NO_VALUE")
UNARY_METHODS = {"__neg__", "__pos__", "__abs__", "__invert__"}


def op(name):
    def func(self, *args, **kwargs):
        return type(self)(name, *args, **kwargs)
    func.__name__ = name
    return func


def _hasattr(obj, name):
    try:
        _getattr(obj, name)
        return True
    except AttributeError:
        return False


def hasattr_(obj, name):
    """Check if object has attribute."""
    if not isinstance(obj, type):
        if _hasattr(obj, name):
            return True
        obj = type(obj)
    return any(map(flip(_hasattr)(name), obj.mro()))


class OldExpr:
    """Expression base class.

    Expression base class will be used to record method calls and their arguments
    in order to be able to replay them later on.

    """

    def __init__(self, name, obj, *args, expr=None, **kwargs):
        self.name = name
        if isinstance(obj, type) or callable(obj):
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
    def __name__(self):
        return _getattr(self, "name")

    @property
    def has_obj(self):
        return _hasattr(self, "_obj")

    @property
    def obj(self):
        """Return the object."""
        if not self.has_obj:
            args = _getattr(self, "args")
            kwargs = _getattr(self, "kwargs")
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

    def _get_obj_or_typ(self):
        if self.has_obj:
            return _getattr(self, "obj")
        return _getattr(self, "typ")


    def __getattribute__(self, name: str) -> Any:
        if _hasattr(type(self), name) or _hasattr(object, name) or \
            (name.startswith("__") and not name.endswith("__")) or \
                name in {"expr", "typ", "__hash__", "_obj", "obj"} or \
                    any(_ in name for _ in {"repr", "mro", "getattr"}):
            return _getattr(self, name)

        arg = self._get_obj_or_typ()
        if _hasattr(arg, name):
            return type(self)(name, arg, expr=self)
        raise AttributeError(f"{arg} has no attribute {name}")

    def _make_new(self, name, *args, expr=None, **kwargs):
        if expr is None:
            expr = self
        arg = self._get_obj_or_typ()
        return type(self)(name, arg, *args, expr=expr, **kwargs)

    def __call__(self, *args, **kwargs):
        name = _getattr(self, "name")
        return self._make_new(name, *args, expr=self.expr, **kwargs)

    def _args_str(self):
        args = _getattr(self, "args")
        if args:
            args = ", ".join([f"'{a}'" if isinstance(a, str) else str(a) for a in args])
        else:
            args = ""
        kwargs = _getattr(self, "kwargs")
        if kwargs:
            kwargs = ", ".join(f"{k}='{v}'" if isinstance(v, str) else f"{k}={v}" for k, v in kwargs.items())
        else:
            kwargs = ""
        args = ", ".join(filter(None, [args, kwargs]))
        return args

    def __str__(self) -> str:
        name = _getattr(self, "name")
        typ = _getattr(self, "typ")
        expr = _getattr(self, "expr")
        args = self._args_str()
        if expr is None:
            if self.has_obj:
                return f"{name} = ({self.obj})"
            return f"{name} = {typ.__name__}({args})"
        if name == "F":
            if _hasattr(self.obj, "__name__"):
                return f"{expr}.{self.obj.__name__}({args})"
            return f"{expr}.F({self.obj})({args})"
        out = f"{expr}.{name}"
        if callable(getattr(typ, name)):
            out += f"({args})"
        return out

    def __repr__(self) -> str:
        name = _getattr(self, "name")
        typ = _getattr(self, "typ")
        expr = _getattr(self, "expr")
        args = self._args_str()
        if self.has_obj:
            arg = str(self.obj)
        else:
            arg = typ.__name__
        args = [arg, args]
        if name != "F":
            args.insert(0, f"'{name}'")
        if expr is not None:
            args.append(f"expr={repr(expr)}")
        args = filter(None, args)
        parts = ", ".join(args)
        if name == "F":
            if _hasattr(typ, "__name__"):
                name = typ.__name__
            else:
                name = repr(typ)
            parts = f"{name}({parts})"
        return f"{type(self).__name__}({parts})"

    def __hash__(self) -> int:
        return hash(repr(self))

    def eval(self, value=None):
        """Evaluate the expression."""
        expr = _getattr(self, "expr")
        if expr is None:
            if value is None:
                return self.obj
            return value
        name = _getattr(self, "name")
        args = _getattr(self, "args")
        kwargs = _getattr(self, "kwargs")
        if name == "F":
            def caller(x):
                return self.typ(x, *args, **kwargs)
        else:
            caller = methodcaller(name, *args, **kwargs)
        return caller(expr.eval(value))

    def F(self, func, *args, **kwargs):
        """Apply a function to the expression."""
        return self._make_new("F", func, *args, **kwargs)

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


def repr_str(obj):
    if isinstance(obj, str):
        return obj
    return repr(obj)


def print_result(func):
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


class Expr:
    """Expression base class.

    Expression base class will be used to record function and method calls and
    their arguments in order to be able to replay them later on.

    >>> e = Expr("e")
    >>> print(e.foo)
    e.foo
    >>> print(e.foo(1, 2, 3))
    e.foo(1, 2, 3)
    >>> print(e.foo(1, 2, 3).bar)
    e.foo(1, 2, 3).bar
    >>> print(e.foo(1, 2, 3).bar(4, 5, 6))
    e.foo(1, 2, 3).bar(4, 5, 6)
    >>> print(e.foo(1, 2, 3).bar.baz)
    e.foo(1, 2, 3).bar.baz
    >>> print(e.F(str.upper))
    str.upper(e)
    >>> print(e.F(str.upper).lower()))
    str.upper(e).lower()
    >>> print(e.F(str.upper).lower().F(str.title))
    str.title(str.upper(e).lower())

    """
    def __init__(self, obj, *args, expr=None, **kw):
        self.obj__ = obj
        self.expr__ = expr
        self.args__ = args
        self.kw__ = kw

    def __getattribute__(self, name):
        if name.startswith("__dask_"):
            raise AttributeError(name)
        if name.endswith("__") and not name.startswith("__") or \
            hasattr_(type(self), name):
            return _getattr(self, name)
        return type(self)(name, expr=self)

    def __getitem__(self, item):
        return type(self)("__getitem__", item, expr=self)

    def __call__(self, *args, **kw):
        return type(self)("__call__", *args, expr=self, **kw)

    def F(self, func, *args, **kw):
        return type(self)(func, *args, expr=self, **kw)

    def __str__(self) -> str:
        obj = self.obj__
        expr = self.expr__
        if obj is None:
            if expr is None:
                return f"{type(self).__name__}()"
            return f"{type(self).__name__}({expr})"
        if expr is None:
            if isinstance(obj, str):
                return obj
            return f"({repr(obj)})"
        args = self.args__
        kw = self.kw__
        if callable(obj) or obj in {"__call__", *UNARY_METHODS}:
            if obj == "__call__":
                name = str(expr)
            elif obj in UNARY_METHODS:
                if args:
                    raise ValueError("Unary method cannot have arguments.")
                name = f"{expr}.{obj}"
            else:
                name = get_name(obj)
                if expr is not None:
                    args = (expr, *args)
            return format_invocation(name, args, kw)
        if isinstance(obj, str):
            if expr is not None:
                out = f"{expr}.{obj}"
            else:
                out = obj
            if args or kw:
                out = format_invocation(out, args, kw)
            return out
        msg = "Expression is not None and obj is not str or callable."
        msg += f"\n{type(obj)=}, obj: {obj!r}"
        msg += f"\nexpr: {expr!r}"
        raise ValueError(msg)

    def __repr__(self) -> str:
        return str(self)

    def __hash__(self) -> int:
        return hash((type(self), repr(self)))

    @staticmethod
    def make_op(name):
        def func(self, *args, **kwargs):
            return type(self)(name, *args, expr=self, **kwargs)
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

    def eval(self, value=NO_VALUE):
        """Evaluate the expression."""
        # pylint: disable=logging-fstring-interpolation
        obj = self.obj__
        expr = self.expr__
        args = self.args__
        kw = self.kw__
        logging.debug(f"eval {obj=}, {expr=}, {value=}")
        if expr is None:
            logging.debug(f"expr is None, {obj=}, {value=}")
            if value is NO_VALUE:
                logging.debug(f"value is NO_VALUE {obj=}, {expr=}")
                if callable(obj):
                    logging.debug("top level is callable, will call: %s", obj)
                    return obj(*args, **kw)
                logging.debug("returning base object: %s", obj)
                return obj
            logging.debug(f"returning value {value=}")
            return value
        logging.debug("evaluating expression")
        prev = expr.eval(value)
        logging.debug(f"evaluated expression {prev=}")
        if callable(obj):
            logging.debug(f"callable {obj=}, {expr=}, {value=}, {prev=}")

            def _caller(x: Any) -> Any:
                logging.debug(f"caller {x=}, {obj=}, {expr=}, {value=}, {prev=}, {args=}, {kw=}")
                return obj(x, *args, **kw)
            caller: Callable = _caller
        elif obj in {"__call__", *UNARY_METHODS}:
            logging.debug(f"magic methodcaller {obj=}, {expr=}, {value=}, {prev=}, {args=}, {kw=}")
            caller: Callable = methodcaller(obj, *args, **kw)
        else:
            logging.debug(f"getattr {obj=}, {expr=}, {value=}, {prev=}")
            attr = getattr(prev, obj)
            logging.debug(f"attr {attr=}")
            if callable(attr) and (args or kw):
                logging.debug(f"callable attr {attr=}, {expr=}, {value=}, {prev=}, {args=}, {kw=}")
                return attr(*args, **kw)
            return attr

        logging.debug(f"eval {obj=}, {expr=}, {value=}, {prev=}")
        return caller(prev)


def get_root_value(expr):
    while expr.expr__ is not None:
        expr = expr.expr__
    return expr.obj__


def get_root_expr(expr):
    while expr.expr__ is not None:
        expr = expr.expr__
    return expr


def reduce_expr(expr):
    exprs = deque()
    while expr.expr__ is not None:
        exprs.insert(0, (expr.obj__, expr.args__, expr.kw__))
        expr = expr.expr__
    exprs.insert(0, (expr.obj__, expr.args__, expr.kw__))
    return tuple(exprs)

def expr_maker(exprs, root=None):
    expr = root
    for obj, args, kw in exprs:
        expr = Expr(obj, *args, **kw, expr=expr)
    return expr


def replace_root_value(expr, value, *args, **kw):
    _, *exprs = reduce_expr(expr)
    exprs = ((value, args, kw), *exprs)
    return expr_maker(exprs)
