import asyncio
import contextvars
import inspect
import os
import logging
import threading
import typing as t

from asyncio.events import AbstractEventLoop
from contextvars import ContextVar
from queue import Queue as ThreadingQueue
from textwrap import dedent as D

from .async_hooks import at_loop_stop_callback

__all__ = ["sync_to_async", "async_to_sync"]


logger = logging.getLogger(__name__)
#############################
### debugging logger boilerplate

if os.environ.get("EXTRAASYNC_DEBUG"):

    logger.setLevel(logging.DEBUG)

    # Create a console handler and set its level to DEBUG
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Create a formatter and attach it to the handler
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    # Attach the handler to the logger
    logger.addHandler(console_handler)


###########################
_poison = object()


class _QueueSet(t.NamedTuple):
    thread: threading.Thread
    queue: ThreadingQueue
    event: threading.Event


class _SyncTask(t.NamedTuple):
    sync_task: t.Callable
    args: tuple
    kwargs: t.Mapping
    loop: AbstractEventLoop
    context: contextvars.Context
    done_future: asyncio.Future


T = t.TypeVar("T")

_non_bridge_loop: ContextVar[t.Optional[AbstractEventLoop]] = ContextVar(
    "non_bridge_loop", default=None
)
_context_bound_task: ContextVar[t.Optional[_SyncTask]] = ContextVar(
    "_context_bound_task", default=None
)


def sync_to_async(
    func: t.Callable[..., T] | t.Coroutine,
    args: t.Sequence[t.Any] = (),
    kwargs: t.Optional[t.Mapping[str, t.Any | None]] = None,
) -> T:
    """Allows calling an async function from a synchronous context.

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

    if inspect.iscoroutine(func):
        if args or kwargs:
            raise TypeError("Can't accept extra arguments for existing coroutine")
        coro = func
    else:
        coro = t.cast(t.Callable, func)(*args, **kwargs)

    root_sync_task = _context_bound_task.get()
    if not root_sync_task:
        return _sync_to_async_non_bridge(coro, args, kwargs)

    loop = root_sync_task.loop
    # context = root_sync_task.context
    sync_thread_context_copy = contextvars.copy_context()
    event = _ThreadPool.get(loop).all[threading.current_thread()].event

    event.clear()
    task: None | asyncio.Task = None  # Keep a strong reference to the task
    inner_exception = None

    def do_it():
        nonlocal task, inner_exception
        logger.debug(
            "Creating task in %s from %s",
            loop,
            thread_name := threading.current_thread().name,
        )
        try:
            task = loop.create_task(coro, context=sync_thread_context_copy)
        except Exception as exc:
            # abort if there is an error creating the task itself!
            event.set()
            inner_exception = exc
            raise
        task.add_done_callback(lambda task, event=event: event.set())

    loop.call_soon_threadsafe(do_it)
    # Pauses sync-worker thread until original co-routine is finhsed in
    # the original event loop:
    event.wait()
    if task is None and inner_exception is None:
        # should be unreachable - but this check keeps the linters quiet!
        raise RuntimeError("SyncToAsync error: assynchronous code not executed")
    if inner_exception:
        raise inner_exception
    if exc := task.exception():
        raise exc
    return task.result()


def _sync_to_async_non_bridge(
    coro: t.Coroutine[t.Any, t.Any, T],
    args: t.Sequence[t.Any],
    kwargs: t.Mapping[str, t.Any],
) -> T:
    loop = _non_bridge_loop.get()
    if not loop:
        try:
            loop = asyncio.new_event_loop()
        except RuntimeError:
            raise RuntimeError(
                D(
                    """\
                Error trying to create a new async loop - to be able to call 'sync_to_async' from
                code inside a running async loop, the parent 'sync execution branch' must be called
                with `extraasync.async_to_sync`.
            """
                )
            )
        _non_bridge_loop.set(loop)

    return loop.run_until_complete(coro)


class _ThreadPool:
    loop: AbstractEventLoop

    pools = dict()

    @classmethod
    def get(cls, loop=None):
        if loop is None:
            loop = asyncio.get_running_loop()
        if loop not in cls.pools:
            cls.pools[loop] = instance = _ThreadPool()
            instance.loop = loop
            at_loop_stop_callback(instance.close_threads, loop=loop)

        return cls.pools[loop]

    def __init__(self):
        self.idle = set[_QueueSet | tuple[()]]()
        self.all = dict()
        self.running: ContextVar[_QueueSet | tuple[()]] = ContextVar(
            "running", default=()
        )

    def __enter__(self):
        if self.idle:
            queue_set = self.idle.pop()
        else:
            logger.debug("Creating new sync-worker thread")
            queue = ThreadingQueue()
            event = threading.Event()
            thread = threading.Thread(target=_sync_worker, args=(queue,))
            queue_set = _QueueSet(thread, queue, event)
            thread.start()
            self.all[thread] = queue_set
        self.running.set(queue_set)
        return queue_set

    def __exit__(self, *exc_data):
        queue_set = self.running.get()
        self.running.set(())
        self.idle.add(queue_set)

    def close_threads(self):
        for queue_set in self.all.values():
            queue_set.event.set()  # unblocks eventually blocked threads
            queue_set.queue.put(_poison)
        self.all.clear()
        self.idle.clear()
        self.running.set(())

    def __repr__(self):
        running = bool(self.running.get())
        return f"<_ThreadPool with {len(self.all)} threads - with {len(self.all) - len(self.idle)} threads in use>"


def _in_context_sync_worker(sync_task: _SyncTask):

    _context_bound_task.set(sync_task)
    try:
        logger.debug("Entering sync call in worker thread")
        result = sync_task.sync_task(*sync_task.args, **sync_task.kwargs)
        logger.debug("Returning from sync call in worker thread")
    finally:
        _context_bound_task.set(None)
    return result


def _sync_worker(queue):
    """Inner function to call sync "tasks" in a separate thread."""
    while True:
        sync_task_bundle = queue.get()
        logger.debug(
            "*" * 100 + "Got new sync call %s in worker-thread %s",
            sync_task_bundle,
            threading.current_thread().name,
        )
        if sync_task_bundle is _poison:
            logger.info(
                "Stopping sync-worker thread %s", threading.current_thread().name
            )
            return
        context = sync_task_bundle.context
        loop = sync_task_bundle.loop
        fut = sync_task_bundle.done_future
        try:
            result = context.copy().run(_in_context_sync_worker, sync_task_bundle)
        except BaseException as exc:
            result = exc
            loop.call_soon_threadsafe(fut.set_exception, result)
        else:
            loop.call_soon_threadsafe(fut.set_result, result)


def async_to_sync(
    func: t.Callable[..., T],
    args: t.Sequence[t.Any] = (),
    kwargs: t.Optional[t.Mapping[str, t.Any | None]] = None,
) -> asyncio.Future[T]:
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
    if task is None:
        raise RuntimeError("async_to_sync called outside of an asyncio task.")
    loop = task.get_loop()
    context = (
        task.get_context()
    )  # it is resposibility of those using the context to copy it!
    # (also, with the "extracontext" package, there are
    # tools to modify an existing context if needed

    done_future = loop.create_future()

    # Using the 'with' block to allocate a thread would return it
    # to the pool before the future was done, triggering a dead lock
    # if a new thread would be needed

    thread_pool = _ThreadPool.get()
    queue_set = thread_pool.__enter__()
    done_future.add_done_callback(
        lambda fut, thread_pool=thread_pool: thread_pool.__exit__(None, None, None)
    )

    # with _ThreadPool.get() as queue_set:
    queue_set.queue.put(
        _SyncTask(func, tuple(args), kwargs, loop, context, done_future)
    )
    logger.debug("Created future awaiting sync result from worker thread for %s", func)

    return done_future
