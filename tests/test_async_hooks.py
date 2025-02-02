import asyncio
import sys

import pytest

from extraasync import at_loop_stop_callback, remove_loop_stop_callback


def test_loop_stop_callback_1():

    executed = done = False

    def cb1():
        nonlocal done
        done = True

    async def blah():
        nonlocal executed
        await asyncio.sleep(0)
        executed = True

    loop = asyncio.new_event_loop()
    at_loop_stop_callback(cb1, loop)

    loop.run_until_complete(blah())

    assert executed
    assert done


def test_loop_stop_callback_multiple_cbs():

    executed = done = done2 = False

    def cb1():
        nonlocal done
        done = True

    def cb2():
        nonlocal done2
        done2 = True

    async def blah():
        nonlocal executed
        await asyncio.sleep(0)
        executed = True

    loop = asyncio.new_event_loop()
    at_loop_stop_callback(cb1, loop)
    at_loop_stop_callback(cb2, loop)

    loop.run_until_complete(blah())

    assert executed
    assert done
    assert done2



def test_loop_stop_callback_remove():

    executed = done = False

    def cb1():
        nonlocal done
        done = True

    async def blah():
        nonlocal executed
        remove_loop_stop_callback(handle)

        await asyncio.sleep(0)
        executed = True

    loop = asyncio.new_event_loop()
    handle = at_loop_stop_callback(cb1, loop)

    loop.run_until_complete(blah())

    assert executed
    assert not done


def test_loop_stop_callback_from_within_loop():

    executed = done = False

    def cb1():
        nonlocal done
        done = True

    async def blah():
        nonlocal executed
        at_loop_stop_callback(cb1)

        await asyncio.sleep(0)
        executed = True


    asyncio.run(blah())

    assert executed
    assert done
