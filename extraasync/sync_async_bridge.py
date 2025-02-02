import asyncio
import contextvars
import inspect

import typing as t


T = t.TypeVar("T")

_non_bridge_loop = contextvars.ContextVar("non_bridge_loop", default=None)
_bridge = contextvars.ContextVar("_bridge", default=None)



def sync_to_async(
    func: t.Callable[[...,], T] | t.Coroutine,
    args: t.Sequence[t.Any] = (),
    kwargs: t.Mapping[str, t.Any | None] = None
) -> T:
    """ Allows calling an async function from a synchronous context.

    When called from a synchronous scenario, this
    will create and cache an asyncio.loop instance per thread for code that otherwise
    is not using asynchronous features. The plain use should permit
    synchronous code to use asynchronous networking libraries
    in a more efficient way than creating a new asyncio.loop at each call.

    When called from an asynchronous scenario, where a synchronous function was
    previously called from an async context, this will allow the nested call to
    happen in the context of the outermost loop, enabling the writting of
    a single code path to both async and asynchronous contexts. In other words,
    and async function which calls a synchronous function by awaiting the "async_to_sync" counterpart to this function can have the synchronous function call back
    asynchronous contexts using this call. This is the so called "bridge mode" [WIP]
    """

    if kwargs is None:
        kwargs = {}
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if not loop:
        return _sync_to_async_non_bridge(func, args, kwargs)
    return NotImplementedError("Bridge mode is WIP")



def _sync_to_async_non_bridge(
    func: t.Callable[[...,], T] | t.Coroutine,
    args: t.Sequence[t.Any],
    kwargs: t.Mapping[str, t.Any]
) -> T:
    loop = _non_bridge_loop.get()
    if not loop:
        loop = asyncio.new_event_loop()
        _non_bridge_loop.set(loop)

    if inspect.iscoroutine(func):
        if args or kwargs:
            raise RuntimeError("Can't accept extra arguments for existing coroutine")
        coro = func
    else:
        coro = func(*args, **kwargs)
    return loop.run_until_complete(coro)


async def async_to_sync(
    func: t.Callable[[...,], T],
    args: t.Sequence[t.Any] = (),
    kwargs: t.Mapping[str, t.Any | None] = None
) -> t.Awaitable[T]:
    if kwargs is None:
        kwargs = {}

    loop = asyncio.get_running_loop()

    task_stack = _TaskGen(func, args, kwargs)
    first = True
    future_result = None
    while True:
        try:
            if first:
                future = task_stack.__next__()
                first = False
            else:
                future = task_stack.send(future_result)
        except StopIteration as stop:
            return stop.value


    return func(*args, **kwargs)


def _taskgen(func, *args, **kwargs):
    inner_gen = _inner_gen()
    bridge_stack = _bridge.get()
    if bridge_stack is None:
        bridge_stack = []
        _bridge.set(bridge_stack)
    bridge_stack.append(inner_gen)


class _TaskGen:
    def __init__(self, func, args, kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __next__(self):
        ...

    def send(...): pass

