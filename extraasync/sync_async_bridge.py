import asyncio
import contextvars
import inspect
import logging
from queue import Queue as ThreadingQueue
import threading
import time

import typing as t

from .async_hooks import at_loop_stop_callback


logger = logging.getLogger(__name__)
#############################
## debugging logger boilerplate
logger.setLevel(logging.DEBUG)

# Create a console handler and set its level to DEBUG
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create a formatter and attach it to the handler
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Attach the handler to the logger
logger.addHandler(console_handler)


###########################

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
    #breakpoint()
    if not root_sync_task:
        return _sync_to_async_non_bridge(func, args, kwargs)

    loop = root_sync_task.loop
    context = root_sync_task.context
    # FIXME: are events "cheap"? Should them be added to the pool, instead
    # of creating one everytime?  (I guess it would be better)
    event = threading.Event()
    event.clear()
    task = None # strong reference to the task
    #breakpoint()
    def do_it():
        nonlocal task
        logger.debug("Creating task in %s from %s", loop, threading.current_thread().name     )
        task = loop.create_task(func(*args, **kwargs), context=context.copy())
        task.add_done_callback(lambda task, event=event: event.set())

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
    def __enter__(self):
        if self.idle:
            queue_set = self.idle.pop()
        else:
            logger.debug("Creating new sync-worker thread")
            queue = ThreadingQueue()
            return_queue = ThreadingQueue()  # FIXME: maybe not needed
            thread = threading.Thread(target=_sync_worker, args=(queue, return_queue))
            queue_set = (thread, queue, return_queue)
            # FIXME: arrange callback to kill thread on loop shutting_down:
            # extraasync.at_loop_stop_callback(loop, lambda queue=queue: queue.put((_poison, None, None, None))
            # (_ThreadPool must be made per-loop)
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
        logger.debug("Entering sync call in worker thread")
        result = sync_task.sync_task(*sync_task.args, **sync_task.kwargs)
        logger.debug("Returning from sync call in worker thread")
    finally:
        _context_bound_loop.set(None)
    return result


def _sync_worker(queue, return_queue):
    """Inner function to call sync "tasks" in a separate thread.


    """
    while True:
        sync_task_bundle = queue.get()
        logger.debug("Got new sync call %s in worker-thread %s", sync_task_bundle, threading.current_thread().name)
        if sync_task_bundle is _poison:
            logger.info("Stopping sync-worker thread %s", threading.current_thread().name)
            return
        context = sync_task_bundle.context
        loop = sync_task_bundle.loop
        fut = sync_task_bundle.done_future
        try:
            result = context.run(_in_context_sync_worker, sync_task_bundle)
        except BaseException as exc:
            result = exc
            loop.call_soon_threadsafe(fut.set_exception, result)
        else:
            loop.call_soon_threadsafe(fut.set_result, result )



def async_to_sync(
    func: t.Callable[[...,], T],
    args: t.Sequence[t.Any] = (),
    kwargs: t.Mapping[str, t.Any | None] = None
) -> t.Awaitable[T]:
    """Returns a future wrapping a synchronous call in other thread

    Most important: synchronous code called this way CAN RESUME
    calling async co-routines in the same loop down the
    call chain by using the pair "sync_to_async" call, unlike
    ordinary "loop.run_in_executor".

    Unlike "run_in_executor" this will create new threads as needed
    in an internall pool,
    so that no async task is stopped waiting for a free new thread
    in the pool. Subsequent, sequential, calls do get to _reuse_ threads
    in this pool.

    Also, the synchronous code is executed in a context copy
    from the current task - so that contextvars will work
    in the synchronous code.

    Returns a future that will contain the results of the synchronous call
    """
    logger.debug("Starting async_to_sync call to %s", func)

    if kwargs is None:
        kwargs = {}

    task = asyncio.current_task()
    loop = task.get_loop()
    context = task.get_context().copy()

    done_future = loop.create_future()

    with _ThreadPool as (_, queue, return_queue):
        logger.info((queue, return_queue))
        queue.put(_SyncTask(func, args, kwargs, loop, context, done_future))
        #await done_future
        logger.debug("Created future awaiting sync result from worker thread for %s", func)

    return done_future

