# SPDX-License-Identifier: CC-PDM-1.0
# author:   Martin Jurča
import asyncio

import typing as t

T = t.TypeVar("T")
R = t.TypeVar("R")

def _as_async_iterable(
    iterable: t.AsyncIterable[T] | t.Iterable[T],
) -> t.AsyncIterable[T]:
    if isinstance(iterable, t.AsyncIterable):
        return iterable

    async def _sync_to_async_iterable() -> t.AsyncIterable[T]:
        for item in iterable:
            yield item

    return _sync_to_async_iterable()


async def pipeline(
    iterable: t.AsyncIterable[T] | t.Iterable[T],
    func: t.Callable[[T], R | t.Awaitable[R]],
    *args,
    max_concurrency: int = 0,
    **kwargs,
) -> t.AsyncIterable[R]:
    """
    Asynchronously map a function over an (a)synchronous iterable with bounded
    parallelism.

    Allows concurrent processing of items from the given iterable, up to a
    specified maximum concurrency level. Takes a function that can be either synchronous
    (returning R) or asynchronous (returning Awaitable[R]) and returns an async
    iterable producing results in input order as soon as they become available.
    Internal queues are bounded, preventing consumption of the entire iterable
    at once in memory.

    Args:
        iterable:
            The source of items to process. Can be a synchronous or asynchronous
            iterable.
        func:
            The mapping function to apply to each item. May be synchronous
            (returning R) or asynchronous (returning Awaitable[R]). All *args
            and **kwargs are forwarded to this function.
        max_concurrency (int):
            Maximum number of concurrent worker tasks. Defaults to 4.
        *args:
            Extra positional arguments passed on to `func`.
        **kwargs:
            Extra keyword arguments passed on to `func`.

    Yields:
        R: The result of applying `func` to each item, in the same order as
        their corresponding inputs.

    Notes:
        - If the callback is synchronous, it will be invoked directly in the
        event loop coroutine, so consider wrapping it with asyncio.to_thread()
        if blocking is significant.
        - This implementation uses internal queues to avoid reading from
        `iterable` too far ahead, controlling memory usage.
        - Once an item finishes processing, its result is enqueued and will be
        yielded as soon as all previous results have also been yielded.
        - If the consumer of this async iterable stops consuming early, workers
        may block while attempting to enqueue subsequent results. It is
        recommended to cancel this coroutine in such case to clean up
        resources if it is no longer needed.
        - If the work for some items is very slow, intermediate results are
        accumulated in an internal buffer until those slow results become
        available, preventing out-of-order yielding.
    """

    input_terminator = t.cast(T, object())
    output_terminator = t.cast(R, object())
    input_queue = asyncio.Queue[tuple[int, T]](max_concurrency)
    output_queue = asyncio.Queue[tuple[int, R]](max_concurrency)
    last_fed_index = -1
    next_to_yield = 0
    early_results: dict[int, R] = {}

    active_worker_tasks = set()
    if max_concurrency:
        active_tasks_semaphore = asyncio.Semaphore(max_concurrency)
        feeder_semaphore = asyncio.Semaphore(max_concurrency)
    else:
        active_tasks_semaphore = None
        feeder_semaphore = None
    async def _work_step(item, index, *args, **kwargs):
        result = exception = None
        try:
            result: R | t.Awaitable[R]= func(item, *args, **kwargs)
        except Exception as exc:
            exception = exc

        if not exception and isinstance(result, t.Awaitable):
            # cast in original code doesn't make sense:
            # mapping callback must await to a "R" otherwise a typing error is justified.
            # result = t.cast(R, await result)
            try:
                result = await result
            except Exception as exc:
                exception = exc
        await output_queue.put((index, result, exception))
        input_queue.task_done()
        #return index, result, exception


    async def _worker() -> None:
        while True:

            index, item, input_exception = await input_queue.get()

            if input_exception or item is input_terminator:
                input_queue.task_done()
                break

            if  active_tasks_semaphore is None or await active_tasks_semaphore.acquire():
                await asyncio.sleep(0)
                task = asyncio.create_task(_work_step(item, index, *args, **kwargs))
                active_worker_tasks.add(task)

                task.add_done_callback(lambda t: (active_worker_tasks.remove(t), active_tasks_semaphore and active_tasks_semaphore.release()))

        await asyncio.gather(*active_worker_tasks)
        await output_queue.put((-1, output_terminator, None))


    async def _feeder() -> None:
        nonlocal last_fed_index

        source = _as_async_iterable(iterable)
        while True:
            exception = item = None
            if feeder_semaphore:
                await feeder_semaphore.acquire()
            try:
                item = await anext(source, input_terminator)
            except Exception as exc:
                # FIXME: maybe clean-up TB and exception info
                # depending on running mode?
                exception = exc

            if item is input_terminator:
                break

            last_fed_index += 1
            await input_queue.put((last_fed_index, item, exception))

        await input_queue.put((-1, input_terminator, None))

    async def _re_orderer() -> t.AsyncIterable[R]:
        nonlocal next_to_yield
        finish = False
        while True:
            index, result, exception = await output_queue.get()
            if result is output_terminator:
                output_queue.task_done()
                finish = True

                return

            early_results[index] = result if exception is None else exception
            while next_to_yield in early_results:

                if feeder_semaphore:
                    feeder_semaphore.release()

                yield early_results.pop(next_to_yield)
                next_to_yield += 1
            output_queue.task_done()

    admin_tasks = {
        asyncio.create_task(_worker()),
        asyncio.create_task(_feeder())
    }

    try:
        async for result in _re_orderer():
            yield result
    finally:
        for task in admin_tasks:
            task.cancel()

    await asyncio.gather(*admin_tasks, return_exceptions=True)


Pipeline = pipeline
