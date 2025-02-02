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

