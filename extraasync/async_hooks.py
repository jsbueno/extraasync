import asyncio
import warnings
import typing as t

from asyncio.events import AbstractEventLoop


DEBUG = False

if not hasattr(asyncio.BaseEventLoop, "_run_forever_cleanup"):

    warnings.warn(
        f"asyncio.BaseEventLoop no longer implements  '_run_forever_cleanup', This means the  "
        "'at_loop_stop_callback' function will fail! Please, add an issue to the project reporting this at "
        "https://github.com/jsbueno/extraasync"
    )


type LoopFinalizerHandle = int


class FunctionWithHookMapping(t.Protocol):
    def __call__(self, *args, **kwargs) -> t.Any: ...

    hooks: dict[LoopFinalizerHandle, t.Callable[[], t.Any]]


class LoopWithFinalizer(t.Protocol):
    _run_forever_cleanup: t.Callable[[], None]


# Registry for all finalizer functions in event-loops which use this helper:
_loop_cleanuppers = {}


def at_loop_stop_callback(
    callback: t.Callable[[], t.Any], loop: t.Optional[AbstractEventLoop] = None
) -> LoopFinalizerHandle:
    """Schedules a callback to when the asyncio loop is stopping

    The callback is called without any parameters
    (use functools.partial if you need any) - and is called
    either at loop shutdown, or when the main task
    in execution with 'loop.run_until_complete' has finsihed.

    More than one callback can be registered in this way -
    they will be called synchronously, in order.

    returns a handle which can be used to unregister
    the callback with 'remove_loop_stop_callback'

    """

    if loop is None:
        loop = asyncio.get_running_loop()

    loop_ = t.cast(LoopWithFinalizer, loop)

    original_clean_up = loop_._run_forever_cleanup

    def new_run_forever_cleanup():
        for handle, cb in cleanup_func.hooks.items():
            try:
                cb()
            except Exception as exc:
                if not DEBUG:
                    warnings.warn(
                        f"""\
                        Supressed Exception raised on loop callback {cb.__name__}:
                            {exc}

                        set extraasync.async_hooks.DEBUG to True
                        to have it raised instead.

                    """
                    )
                else:
                    raise exc
        original_clean_up()

    new_run_forever_cleanup = t.cast(FunctionWithHookMapping, new_run_forever_cleanup)

    if loop not in _loop_cleanuppers:
        new_run_forever_cleanup.hooks = {}
        _loop_cleanuppers[loop] = new_run_forever_cleanup
        loop_._run_forever_cleanup = new_run_forever_cleanup

    cleanup_func = _loop_cleanuppers[loop]
    new_cb_key = (
        max(
            (key for key in cleanup_func.hooks.keys() if isinstance(key, int)),
            default=0,
        )
        + 1
    )
    cleanup_func.hooks[t.cast(LoopFinalizerHandle, new_cb_key)] = callback
    return new_cb_key


def remove_loop_stop_callback(
    handle: LoopFinalizerHandle, loop: t.Optional[AbstractEventLoop] = None
) -> None:
    """Removes a scheduled callback for when the loop stops.

    If the handle or loop doesn't exist, simply errors out.
    """
    if loop is None:
        loop = asyncio.get_running_loop()
    cleanup_hooks = _loop_cleanuppers[loop].hooks
    del cleanup_hooks[handle]
