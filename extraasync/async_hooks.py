import asyncio
import threading
import typing as t
import sys
import warnings

from asyncio import events
from asyncio.events import AbstractEventLoop



DEBUG = False


if (3, 12) <= sys.version_info < (3, 13):
    # Monkey Patch Python 3.12 BaseEventLoop which refactors
    # loop cleanup code in a method we can hook on!

    class Py313BaseEventLoop:
        """
        Code from cPython 3.13 - licensing rules apply
        Code in this class under the
        PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2

        This is a class stub just to hold the methods to be monkeypatched
        into Python's asyncio BaseEventLoop

        """
        # Lib/asyncio/base_events.py
        def _run_forever_setup(self):
            """Prepare the run loop to process events.

            This method exists so that custom event loop subclasses (e.g., event loops
            that integrate a GUI event loop with Python's event loop) have access to all the
            loop setup logic.
            """
            self._check_closed()
            self._check_running()
            self._set_coroutine_origin_tracking(self._debug)

            self._old_agen_hooks = sys.get_asyncgen_hooks()
            self._thread_id = threading.get_ident()
            sys.set_asyncgen_hooks(
                firstiter=self._asyncgen_firstiter_hook,
                finalizer=self._asyncgen_finalizer_hook
            )

            events._set_running_loop(self)

        def _run_forever_cleanup(self):
            """Clean up after an event loop finishes the looping over events.

            This method exists so that custom event loop subclasses (e.g., event loops
            that integrate a GUI event loop with Python's event loop) have access to all the
            loop cleanup logic.
            """
            self._stopping = False
            self._thread_id = None
            events._set_running_loop(None)
            self._set_coroutine_origin_tracking(False)
            # Restore any pre-existing async generator hooks.
            if self._old_agen_hooks is not None:
                sys.set_asyncgen_hooks(*self._old_agen_hooks)
                self._old_agen_hooks = None

        def run_forever(self):
            """Run until stop() is called."""
            self._run_forever_setup()
            try:
                while True:
                    self._run_once()
                    if self._stopping:
                        break
            finally:
                self._run_forever_cleanup()

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        ok_to_patch = True
    else:
        ok_to_patch = False

    if not ok_to_patch:
        raise RuntimeError("Python 3.12 requires patching asyncio before the async loop starts."
            " Please import extraasync before setting up your async loop"
        )
    asyncio.base_events.BaseEventLoop._run_forever_setup = Py313BaseEventLoop._run_forever_setup
    asyncio.base_events.BaseEventLoop._run_forever_cleanup = Py313BaseEventLoop._run_forever_cleanup
    asyncio.base_events.BaseEventLoop.run_forever = Py313BaseEventLoop.run_forever

    # Now, it is "B.A.U."



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
