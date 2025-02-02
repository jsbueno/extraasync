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

    asyncio.run(blih())
    assert done and done and done3

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
    assert done and done and done3
