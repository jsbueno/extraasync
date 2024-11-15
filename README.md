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
    
