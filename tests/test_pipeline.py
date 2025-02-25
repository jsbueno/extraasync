import asyncio
import warnings

from unittest import mock

import pytest

from extraasync import Pipeline



@pytest.mark.asyncio
async def test_pipeline_produces_results():
    async def producer(n, interval=0):
        for i in range(n):
            yield i
            await asyncio.sleep(interval)

    async def map_function(n, map_interval=0):
        await asyncio.sleep(0)
        return n * 2

    results = []
    async for result in Pipeline(producer(10), map_function):
        results.append(result)

    assert results == list(range(0, 20, 2))

@pytest.mark.parametrize(
    ["interval", "map_interval", "max_concurrency"], [
        (0, .025, 4),
        (0, .025, 2),
        (0, .025, 8),
])
@pytest.mark.asyncio
async def test_pipeline_matches_desired_concurrency(interval, map_interval, max_concurrency):
    concurrent_tasks = 0
    max_concurrent_tasks = 0
    async def producer(n, interval=0):
        for i in range(n):
            yield i
            await asyncio.sleep(interval)

    async def map_function(n):
        nonlocal concurrent_tasks, max_concurrent_tasks
        concurrent_tasks += 1
        max_concurrent_tasks = max(max_concurrent_tasks, concurrent_tasks)
        await asyncio.sleep(map_interval)
        concurrent_tasks -= 1
        return n * 2

    results = []
    async for result in Pipeline(producer(10), map_function, max_concurrency=max_concurrency):
        results.append(result)

    assert results == list(range(0, 20, 2))
    assert max_concurrent_tasks == max_concurrency



@pytest.mark.asyncio
async def test_pipeline_dont_stall_on_task_exception():
    async def producer(n, interval=0):
        for i in range(n):
            yield i
            await asyncio.sleep(interval)

    async def map_function(n):
        raise RuntimeError()
        return n * 2

    results = []
    try:
        async with asyncio.timeout(0.1):
            try:
                async for result in Pipeline(producer(1), map_function):
                    results.append(result)
            except *Exception:
                # for this specific test, any other outcome is good.
                pass

    except TimeoutError:
        assert False, "Pipeline had stalled on inner task exception"

    assert True






