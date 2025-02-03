Extra Async
===========


Utilities for Python Asynchronous programming


aenumerate
-----------

Asynchronous version of Python's "enumerate". 
Just pass an async-iterator where the iterator would be,
and use it in an async-for. 


usage:

```python
import asyncio

from extraasync import aenumerate

async def paused_pulses(n, message="pulse", interval=0.1):
    for i in range(n):
        asyncio.sleep(interval)
        yield message

async def main():
    for index, message in aenumerate(paused_pulses(5)):
        print(index, message)

asyncio.run(main())
```


ExtraTaskGroup
-------------------

An asyncio.TaskGroup subclass that won't cancel all tasks
when any of them errors out.

The hardcoded behavior of asyncio.TaskGroup is that when
any exception is raised in any of the taskgroup tasks,
all sibling incomplete tasks get cancelled immediatelly.

With ExtraTaskGroup, all created tasks are run to completion,
and any exceptions are bubbled up as ExceptionGroups on
the host task.

```python
import asyncio

from extraasync import ExtraTaskGroup

async def worker(n):
    await asyncio.sleep(n/10)
    if n % 3 == 0:
        raise RuntimeError()
    return n

async def main():
    try:
        async with ExtraTaskGroup() as tg:
            tasks = [tg.create_task(worker(i)) for i in range(10)]
    except *RuntimeError as exceptions:
        print(exceptions)

asyncio.run(main())


```

sync_to_async
----------------------
Allows calling an async function from a synchronous context.

When called from a synchronous scenario, this
will create and cache an asyncio.loop instance per thread for code that otherwise
is not using asynchronous features. The plain use should permit
synchronous code to use asynchronous networking libraries
in a more efficient way than creating a new asyncio.loop at each call.

When called in an asynchronous scenario, where a synchronous function was
previously called from an async context **using the `async_to_sync`** call,
this will allow the nested call to  happen in the context of that original loop.
This enables the creation of
a single code path to both async and asynchronous contexts. In other words,
and async function which calls a synchronous function by awaiting the "async_to_sync" counterpart to this function can have the synchronous function call back
asynchronous contexts using this call. This is the so called "bridge mode"

```python

async def inner_async_function():
    """Runs in the same thread and loop as 'async_entry' """
    # await long async client API call
    await asyncio.sleep(0.01)
    return 23

def synchronous_shared_code_path(backend):
    if backend == "async":
        result = sync_to_async(inner_async_function)
    else:
        ...
        result = sync_call()

    return result

async def async_entry():
    # runs in a transparently created thread:
    result = await async_to_sync(synchronous_shared_code_path, kwargs={"backend": "async"}))

result = sync_to_async(blih)  # can be used in place of `asyncio.run`:
                        # for simple, non nested calls, the same
                        # asyncio loop is reused across calls.
assert result == 23

```

async_to_sync
----------------------


Returns a future wrapping a synchronous call in other thread

In a similar way to what `asyncio.loop.run_in_executor` do,
but with improvements:

Most important: synchronous code called this way CAN RESUME
calling async co-routines in the same loop down the
call chain by using the pair "sync_to_async" call, unlike
ordinary "loop.run_in_executor".

Unlike "run_in_executor" this will create new threads as needed
in an internal pool,
so that no async task is stopped waiting for a free new thread
in the pool. Subsequent, sequential, calls do get to _reuse_ threads
in this pool. This uncapped creation of threads is needed, because
in case of a chained context switching between sync and async contexts
in a call stack, not having an available thread would lead to a deadlock.

Also, the synchronous code is executed in a context (contextvars) copy
from the current task - so that contextvars will work
in the synchronous code. In a plain thread, as those used by
the executor call, the sync code will see an empty context.

Returns a future that will contain the results of the synchronous call.

(check example snippet on `sync_to_async` description, above)


at_loop_stop_callback
---------------------------------------------
Schedules a callback to when the asyncio loop is stopping

at_loop_stop_callback(callback, loop=None)

The callback is called without any parameters
(use functools.partial if you need any) - and is called
either at loop shutdown, or when the main task
in execution with 'loop.run_until_complete' has finsihed.

More than one callback can be registered in this way -
they will be called synchronously, in order.

returns a handle which can be used to unregister
the callback with 'remove_loop_stop_callback'.

This project itself makes use of it to stop worker
threads when an event loop is stopped.

It works by patching a private callback in asyncio.BaseEventLoop


remove_loop_stop_callback
---------------------------------------------
Removes a scheduled callback for when the loop stops.


remove_loop_stop_callback(handle, loop=None)

This will simply cancel a previously registred callback
for the current, or explicit loop.
