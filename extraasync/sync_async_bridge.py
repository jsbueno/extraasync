import asyncio
import contextvars
import inspect
import queue
import threading

import typing as t


T = t.TypeVar("T")

_non_bridge_loop = contextvars.ContextVar("non_bridge_loop", default=None)
#_bridge = contextvars.ContextVar("_bridge", default=None)
_worker_threads = {}

_context_bound_loop = contextvars.ContextVar("_context_bound_loop", default=None)
_bridged_tasks = set()


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
    previously called from an async context **using the async_to_sync** call,
    this will allow the nested call to
    happen in the context of that original loop. This enables the creation of
    a single code path to both async and asynchronous contexts. In other words,
    and async function which calls a synchronous function by awaiting the "async_to_sync" counterpart to this function can have the synchronous function call back
    asynchronous contexts using this call. This is the so called "bridge mode" [WIP]
    """

    if kwargs is None:
        kwargs = {}
    root_sync_task = _context_bound_loop.get()
    if not root_sync_task:
        return _sync_to_async_non_bridge(func, args, kwargs)

    loop = root_sync_task.loop
    context = root_sync_task.context
    # FIXME: are events "cheap"? Should them be added to the pool, instead
    # of creating one everytime?  (I guess it would be better)
    event = threading.Event()
    event.clear()
    task = None # strong reference to the task
    def do_it():
        nonlocal task
        task = loop.create_task(func(args, kwargs), context=context)
        task.add_done_callback(event.set)

    loop.call_soon_threadsafe(do_it)
    # Pauses sync-worker thread until original co-routine is finhsed in
    # the original event loop:
    event.wait()
    if exc:=task.exception():
        raise exc
    return task.result()



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


class _ThreadPool:
    def __init__(self):
        self.idle = set()
        self.running = contextvars.ContextVar("running", default=())

        #task = asyncio.current_task()
        #loop = task.get_loop()
        #context = task.get_context()
    def __enter__(enter):
        if self.idle:
            queue_set = self.idle.pop()
        else:
            queue = queue.Queue()
            return_queue = queue.Queue()
            thread = threading.Thread(target=_sync_worker, args=(queue, return_queue))
            queue_set = (thread, queue, return_queue)
            # TODO: arrange callback to kill thread on loop shutting_down:
            # extraasync.at_loop_shutdown(loop, lambda queue=queue: queue.put((_poison, None, None, None))
            thread.start()
        if (running := self.running.get()) == ():
            self.running.set(running := [])
        running.append(queue_set)
        return queue_set

    def __exit__(self, *exc_data):
        queue_set = self.running.get().pop()
        self.idle.add(queue_set)

    def __repr__(self):
        return f"<_ThreadPool with {len(self.idle)} idle  and {len(self.running.get())} threads>"


_ThreadPool = _ThreadPool()


_poison = object()
_sucess, _failure = 1, 0

class _SyncTask(t.NamedTuple):
    sync_task: t.Callable
    args: tuple
    kwargs: dict
    loop: asyncio.BaseEventLoop
    context: contextvars.Context
    done_future: asyncio.Future


def _in_context_sync_worker(sync_task: _SyncTask):

    _context_bound_loop.set(sync_task)
    try:
        result = sync_task.sync_task(*sync_task.args, **sync_task.kwargs)
    finally:
        _context_bound_loop.set(None)
    return result


def _sync_worker(queue, return_queue):
    """Inner function to call sync "tasks" in a separate thread.


    """
    while True:
        sync_task_bundle = queue.get()
        if sync_task is _poison:
            return
        try:

            result = context.run(_in_context_sync_worker, sync_task_bundle)
            status = _sucess
        except BaseException as exc:
            result = exc
            status = _failure
        loop = sync_task_bundle.loop
        fut = sync_task_bundle.done_future
        loop.call_soon_threadsafe(fut.set_result, (status, result) )
        #return_queue.put((status, result))


async def _sync_task_runner(func, args, kwargs):
    task = asyncio.current_task()
    loop = task.get_loop()
    context = task.get_context()
    done_future = loop.create_future()

    with _ThreadPool as (_, queue, return_queue):
        queue.put(_SyncTask(func, args, kwargs, loop, contextm, done_future))
        #await done_future
        status, result = await done_future

    if status == _failure:
        raise result
    return result


async def async_to_sync(
    func: t.Callable[[...,], T],
    args: t.Sequence[t.Any] = (),
    kwargs: t.Mapping[str, t.Any | None] = None
) -> t.Awaitable[T]:
    if kwargs is None:
        kwargs = {}

    loop = asyncio.get_running_loop()
