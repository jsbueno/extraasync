import asyncio
import sys

import pytest

from extraasync import aenumerate


DELAY = sys.getswitchinterval()

@pytest.mark.asyncio
async def test_aenumerate_works():

    async def counter(n):
        for i in range(n):
            yield i
            await asyncio.sleep(DELAY)

    async for i, step in aenumerate(counter(4)):
        assert i == step

