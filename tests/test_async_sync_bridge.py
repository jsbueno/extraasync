import asyncio

import pytest

from extraasync import sync_to_async, async_to_sync

def test_plain_sync_to_async():

    done = False
    async def blah():
        nonlocal done
        await asyncio.sleep(0.01)
        done = True
    sync_to_async(blah)
    assert done


def test_plain_sync_to_async_with_coroutine():

    done = False
    async def blah():
        nonlocal done
        await asyncio.sleep(0.01)
        done = True
    sync_to_async(blah())  # <- pass a co-routine directly
    assert done



def test_async_sync_plain():
    # this is actually a no-op,
    # but for the fact the sync function
    # now has to be awaited.

    done = False

    def blah():
        nonlocal done
        done = True

    async def bleh():
        await async_to_sync(blah)
    asyncio.run(bleh())

    assert done


def test_async_sync_bridge_1():
    # Debugging prints and helpers
    # left on purpose. They might be needded
    # when developping new features

    done = done2 = done3 = False

    # Useful for debugging
    #async def heartbeat():
        #counter = 0
        #while True:
            #await asyncio.sleep(1)
            #print(counter, asyncio.all_tasks())
            #counter += 1

    async def blah():
        nonlocal done
        #print("@" * 100)
        await asyncio.sleep(0.01)
        done = True

    def bleh():
        nonlocal done2
        #print("#" * 100)
        sync_to_async(blah)
        done2 = True

    async def blih():
        nonlocal done3
        #t = asyncio.create_task(heartbeat())
        #print("*" * 100)
        await async_to_sync(bleh)
        #t.cancel()
        done3 = True

    asyncio.run(blih())
    assert done and done2 and done3


def test_async_sync_bridge_2():
    done = done2 = done3 = False

    async def blah():
        nonlocal done
        await asyncio.sleep(0.01)
        done = True

    def bleh():
        nonlocal done2
        sync_to_async(blah)
        done2 = True

    async def blih():
        nonlocal done3
        await async_to_sync(bleh)
        done3 = True

    sync_to_async(blih)
    assert done and done2 and done3

def test_async_sync_bridge_depth2_chain_call():

    done = done2 = done3 = done4 = done5 = False

    async def blah():
        nonlocal done
        await asyncio.sleep(0.01)
        done = True

    def bleh():
        nonlocal done2
        sync_to_async(blah)
        done2 = True

    async def blih():
        nonlocal done3
        await async_to_sync(bleh)
        done3 = True

    def bloh():
        nonlocal done4
        sync_to_async(blih)
        done4 = True

    async def bluh():
        nonlocal done5
        await async_to_sync(bloh)
        done5 = True

    asyncio.run(bluh())
    assert done and done2 and done3 and done4 and done5
