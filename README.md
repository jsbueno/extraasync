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

    
