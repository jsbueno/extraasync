import asyncio
import warnings
import time

from unittest import mock

import pytest

from extraasync import Pipeline, RateLimiter


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


@pytest.mark.asyncio
async def test_pipeline_works_sync_stage():
    async def producer(n, interval=0):
        for i in range(n):
            yield i
            await asyncio.sleep(interval)

    def map_function(n, map_interval=0):
        return n * 2

    results = []
    async for result in Pipeline(producer(10), map_function):
        results.append(result)

    assert results == list(range(0, 20, 2))


@pytest.mark.asyncio
async def test_pipeline_works_with_custom_class_stage():
    async def producer(n, interval=0):
        for i in range(n):
            yield i
            await asyncio.sleep(interval)

    class Map:
        def __init__(self, n, map_interval=0):
            self.n = n

        def __await__(self):
            yield None
            return self.n * 2

    results = []
    async for result in Pipeline(producer(10), Map):
        results.append(result)

    assert results == list(range(0, 20, 2))


@pytest.mark.asyncio
async def test_pipeline_awaiting_it_executes_it():
    results = []

    async def producer(n, interval=0):
        for i in range(n):
            yield i
            await asyncio.sleep(interval)

    async def map_function(n, map_interval=0):
        await asyncio.sleep(0)
        results.append(n * 2)

    ret_val = await Pipeline(producer(10), map_function)

    assert ret_val is None
    assert results == list(range(0, 20, 2))


class _Acumulator:
    def __init__(self, initial=()):
        self.data = []
        self.data.extend(initial)

    def __rrshift__(self, other):
        self.data.append(other)

    def __eq__(self, other):
        return self.data == other.data

    def __repr__(self):
        return f"Acumulator({self.data})"


litlist = [0, 2, 4, 6, 8]
litset = set(litlist)


@pytest.mark.parametrize(
    ["sink_instance", "expected"],
    [
        (None, None),
        (
            [],
            litlist,
        ),
        (
            set(),
            litset,
        ),
        (_Acumulator(), _Acumulator(litlist)),
    ],
)
@pytest.mark.asyncio
async def test_pipeline_awaiting_fills_sink(sink_instance, expected):
    results = []

    async def producer(n, interval=0):
        for i in range(n):
            yield i
            await asyncio.sleep(interval)

    async def map_function(n, map_interval=0):
        await asyncio.sleep(0)
        return n * 2

    results = await Pipeline(producer(5), map_function, sink=sink_instance)

    assert results == expected


@pytest.mark.asyncio
async def test_pipeline_call_results_process_everything():
    async def producer(n, interval=0):
        for i in range(n):
            yield i
            await asyncio.sleep(interval)

    async def map_function(n, map_interval=0):
        await asyncio.sleep(0)
        return n * 2

    results = await Pipeline(producer(10), map_function).results()

    assert results == list(range(0, 20, 2))


@pytest.mark.asyncio
async def test_pipeline_2_stages():
    async def producer(n, interval=0):
        for i in range(n):
            yield i
            await asyncio.sleep(interval)

    async def map_function(n, map_interval=0):
        await asyncio.sleep(0)
        return n * 2

    async def f2(v):
        return v + 1

    results = []
    async for result in Pipeline(producer(10), map_function, f2):
        results.append(result)

    assert results == [n * 2 + 1 for n in range(10)]


@pytest.mark.asyncio
async def test_pipeline_stages_execute_conturrently():
    task_amount = 10
    map_interval = 0.025

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
    async for result in Pipeline(
        producer(task_amount), map_function, max_concurrency=None
    ):
        results.append(result)

    assert results == list(range(0, 2 * task_amount, 2))
    assert max_concurrent_tasks == task_amount


@pytest.mark.parametrize(
    ["task_amount", "interval", "map_interval", "max_concurrency"],
    [
        (10, 0, 0.025, 4),
        (10, 0, 0.025, 2),
        (10, 0, 0.025, 8),
        (200, 0, 0.001, 1),
        (1000, 0, 0.01, 10),
        (1000, 0, 0.1, 200),
        (100, 0, 0.1, 0),
    ],
)
@pytest.mark.asyncio
async def test_pipeline_matches_desired_concurrency(
    task_amount, interval, map_interval, max_concurrency
):
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
    async for result in Pipeline(
        producer(task_amount), map_function, max_concurrency=max_concurrency
    ):
        results.append(result)

    assert results == list(range(0, 2 * task_amount, 2))
    if max_concurrency:
        assert max_concurrent_tasks == max_concurrency
    else:
        assert max_concurrent_tasks == task_amount


@pytest.mark.skip
@pytest.mark.parametrize(
    ["task_amount", "interval", "map_interval", "max_concurrency"],
    [
        (5, 0, 0.1, 1),
        (10, 0, 0.025, 4),
        (10, 0, 0.025, 2),
        (10, 0, 0.025, 8),
        (100, 0, 0.1, 20),
    ],
)
@pytest.mark.asyncio
async def test_pipeline_concurrency_limit_throtles_feeder(
    task_amount, interval, map_interval, max_concurrency
):
    concurrent_tasks = 0
    max_concurrent_tasks = 0

    feeder_requested_times = dict()
    feeder_pressure_errors = []

    async def producer(n, interval=0):
        for i in range(n):
            now = time.monotonic()
            if max_concurrency and i >= max_concurrency + 1:
                if any(
                    (now - feeder_requested_times[j]) < (interval + map_interval)
                    for j in range(0, i - max_concurrency - 1)
                ):
                    try:
                        distance = min(
                            (
                                now - feeder_requested_times[j],
                                j,
                            )
                            for j in range(0, i - max_concurrency - 1)
                        )
                        feeder_pressure_errors.append(
                            f"feeder item {i} called too early: {distance[0]} seconds from item {distance[1]}"
                        )
                    except Exception as exc:
                        feeder_pressure_errors.append(str(exc))
            feeder_requested_times[i] = now
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
    async for result in Pipeline(
        producer(task_amount), map_function, max_concurrency=max_concurrency
    ):
        results.append(result)

    if max_concurrency:
        assert not feeder_pressure_errors, "\n".join(feeder_pressure_errors)
    assert results == list(range(0, 2 * task_amount, 2))


@pytest.mark.asyncio
async def test_pipeline_keeps_order():
    async def generator(n):
        for i in range(n):
            yield i

    async def stage(value):
        # ensure out of order results are yielded
        await asyncio.sleep(int(f"{value:02d}"[::-1]) / 200)
        return value

    assert await Pipeline(generator(100), stage, preserve_order=True).results() == list(
        range(100)
    )


@pytest.mark.asyncio
async def test_pipeline_doesnt_keep_order():
    async def generator(n):
        for i in range(n):
            yield i

    async def stage(value):
        # ensure out of order results are yielded
        await asyncio.sleep(int(f"{value:02d}"[::-1]) / 200)
        return value

    result = await Pipeline(generator(100), stage, preserve_order=False).results()
    assert sum(int(result[i] != i) for i in range(100)) > 10


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
                async for result in Pipeline(
                    producer(1), map_function, on_error="strict"
                ):
                    results.append(result)
            except* Exception:
                # for this specific test, any other outcome is good.
                pass

    except TimeoutError:
        assert False, "Pipeline had stalled on inner task exception"

    assert True


@pytest.mark.asyncio
async def test_pipeline_dont_stall_on_producer_exception():
    async def producer(n, interval=0):
        await asyncio.sleep(interval)
        raise RuntimeError()
        for i in range(n):
            yield i
            await asyncio.sleep(interval)

    async def map_function(n):
        return n * 2

    results = []
    try:
        async with asyncio.timeout(0.1):
            try:
                async for result in Pipeline(producer(1), map_function):
                    results.append(result)
            except* Exception:
                # for this specific test, any other outcome is good.
                pass

    except TimeoutError:
        assert False, "Pipeline had stalled on inner task exception"

    assert True


@pytest.mark.asyncio
async def test_pipeline_concurrency_rate_limit_single_stage():
    rate_limit = 20  # /second
    task_amount = 12

    async def producer(n, interval=0):
        for i in range(n):
            yield i
            await asyncio.sleep(interval)

    async def map_function(n):
        await asyncio.sleep(0)
        return n * 2

    threshold = 0.005
    loop = asyncio.get_running_loop()
    start_time = loop.time()
    results = []
    async for result in Pipeline(
        producer(task_amount), map_function, rate_limit=rate_limit
    ):
        results.append(result)

    elapsed = loop.time() - start_time
    assert elapsed >= 0.5, "11 Tasks should take at least 0.5 seconds at 20/sec"
    assert set(results) == set(range(0, 2 * task_amount, 2))


@pytest.mark.parametrize(
    ["task_ammount", "failing_steps", "two_stages", "preserve_order"],
    [
        (
            10,
            {
                5,
            },
            False,
            True,
        ),
        (
            10,
            {
                5,
            },
            True,
            True,
        ),
        (
            10,
            {
                5,
            },
            True,
            False,
        ),
        (
            10,
            {
                5,
            },
            False,
            False,
        ),
        (10, {1, 3, 6, 9}, False, True),
    ],
)
@pytest.mark.asyncio
async def test_pipeline_runs_to_completion_when_ignoring_exceptions(
    task_ammount, failing_steps, two_stages, preserve_order
):

    async def producer(n, interval=0):
        for i in range(n):
            yield i
            await asyncio.sleep(interval)

    async def map_function(n):
        if n in failing_steps:
            raise ValueError()
        await asyncio.sleep(0)
        return n * 2

    async def second_stage(n):
        await asyncio.sleep(0)
        return n

    use_second_stage = (second_stage,) if two_stages else ()

    results = []

    try:
        async with asyncio.timeout(0.1):
            async for result in Pipeline(
                producer(10),
                map_function,
                *use_second_stage,
                preserve_order=preserve_order,
                on_error="ignore",
            ):
                results.append(result)
    except TimeoutError:

        assert False, f"Timed out waiting pipeline to complete. Results: {results}"

    expected = [
        item * 2 for item in range(0, task_ammount) if item not in failing_steps
    ]
    if not preserve_order:
        results = set(results)
        expected = set(expected)

    assert results == expected


@pytest.mark.skip
@pytest.mark.asyncio
async def test_pipeline_max_simultaneous_record_limit(): ...


@pytest.mark.skip
@pytest.mark.asyncio
async def test_pipeline_can_accept_source_from_rshift_op(): ...


@pytest.mark.skip
@pytest.mark.asyncio
async def test_pipeline_can_chain_new_source_with_rshift_op(): ...


@pytest.mark.skip
@pytest.mark.asyncio
async def test_pipeline_add_stage_pipe_operator(): ...


@pytest.mark.skip
@pytest.mark.asyncio
async def test_pipeline_add_data_and_execute_l_rhift_operator(): ...


@pytest.mark.skip
@pytest.mark.asyncio
async def test_pipeline_store_result_r_rshift_operator(): ...


@pytest.mark.skip
@pytest.mark.asyncio
async def test_pipeline_fine_tune_stages(): ...


@pytest.mark.asyncio
async def test_rate_limiter_starts_immediately():
    loop = asyncio.get_running_loop()
    threshold = 0.005  # ~sys.getswitchinterval()
    limiter = RateLimiter(1)
    start_time = loop.time()
    await limiter
    assert loop.time() - start_time < threshold


@pytest.mark.asyncio
async def test_rate_limiter_throtles_rate():
    loop = asyncio.get_running_loop()
    threshold = 0.005  # ~sys.getswitchinterval()
    limiter = RateLimiter(20, "second")
    start_time = loop.time()
    for i in range(11):
        await limiter
    assert (
        loop.time() - start_time >= 0.5
    )  # should be equal or greater than half second


@pytest.mark.asyncio
async def test_rate_limiter_throtles_rate_if_called_concurrently():
    loop = asyncio.get_running_loop()
    threshold = 0.02  # ~sys.getswitchinterval()
    limiter = RateLimiter(20, "second")
    start_time = loop.time()

    async def limited_task():
        await limiter
        return None

    tasks = set(asyncio.create_task(limited_task()) for _ in range(11))
    start_time = loop.time()
    await asyncio.gather(*tasks)

    elapsed = loop.time() - start_time
    assert elapsed >= 0.5, "should be equal or greater than half second"
    assert elapsed <= 0.5 + threshold, "ellapsed time too long"


@pytest.mark.asyncio
async def test_rate_limiter_copyed_throtles_independently():
    from copy import copy

    loop = asyncio.get_running_loop()
    threshold = 0.02  # ~sys.getswitchinterval()
    limiter1 = RateLimiter(20, "second")
    limiter2 = copy(limiter1)
    start_time = loop.time()

    async def limited_task(limiter):
        await limiter
        return None

    tasks = set(asyncio.create_task(limited_task(limiter1)) for _ in range(11))
    tasks.update(set(asyncio.create_task(limited_task(limiter2)) for _ in range(11)))
    start_time = loop.time()
    await asyncio.gather(*tasks)

    elapsed = loop.time() - start_time
    assert limiter2.rate_limit == limiter1.rate_limit
    assert elapsed >= 0.5, "should be equal or greater than half second"
    assert elapsed <= 0.5 + threshold, "ellapsed time too long"
