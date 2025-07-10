# SPDX-License-Identifier: LGPL-3.0+
# author:   Martin Jurča, Joao S. O. Bueno
import asyncio
from copy import copy
from functools import partial
from logging import getLogger
from inspect import isawaitable
from itertools import chain
from collections.abc import MutableSet, MutableSequence
from numbers import Real as NReal  # for typing purposes
from decimal import Decimal

import inspect
import heapq
import queue

from .sync_async_bridge import async_to_sync

import typing as t

# for some reason, decimal is not a subtype of real.
Real = NReal | Decimal

logger = getLogger(__name__)

T = t.TypeVar("T")
R = t.TypeVar("R")

TIME_UNIT = (
    t.Literal["second"] | t.Literal["minute"] | t.Literal["hour"] | t.Literal["day"]
)


# sentinels:
EOD = object()
EXC_MARKER = object()

try:
    from functools import Placeholder  # new in Python 3.14
except ImportError:
    # Pickleable sentinel:

    class Placeholder:
        pass


class SupportsRShift(t.Protocol):
    def __rrshift__(self, other: t.Any) -> t.Any: ...


class Heap:
    def __init__(self):
        self.data = []

    def peek(self) -> int | None:
        if not self.data:
            return None
        return self.data[0][0]

    def push(self, item: tuple[int, t.Any]):
        heapq.heappush(self.data, item)

    def pop(self):
        return heapq.heappop(self.data)

    def __bool__(self):
        return bool(self.data)

    def __repr__(self):
        return f"<Heap {self.data!r}>"


class AutoSet(MutableSet):
    """Set with an associated asyncio.Queue

    Whenever an item is removed/discarded, an item is fetched from the queue as
    a task factory-no argument callable: it is called and added to the set.

    """

    def __init__(self, initial=()):
        self.data = set(initial)
        self.queue = asyncio.Queue()

    def __contains__(self, value):
        return self.data.__contains__(value)

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def add(self, value):
        self.data.add(value)

    def discard(self, value):
        self.data.discard(value)
        try:
            task_factory = self.queue.get_nowait()
        except asyncio.queues.QueueEmpty:
            return
        self.add(task_factory())


def _as_async_iterable(
    iterable: t.AsyncIterable[T] | t.Iterable[T],
) -> t.AsyncIterable[T]:
    # author:   Martin Jurča
    # License: CC-PDM-1.0
    if isinstance(iterable, t.AsyncIterable):
        return iterable

    async def _sync_to_async_iterable() -> t.AsyncIterable[T]:
        for item in iterable:
            yield item

    return _sync_to_async_iterable()


PipelineErrors = t.Literal["eager", "ignore", "group"]


class RateLimiter:
    """Intended to limit rates for running a given Stage -

    Use, for example, to respect the rate limit of
    external APIs.

    Just await the instance before executing each action that should be throttled.
    """

    # This is offset to a separate class so that it can be plugable
    # (e.g. for  an off-process coordinated limiter)
    def __init__(self, rate_limit: Real, unit: TIME_UNIT = "second"):
        self.rate_limit = rate_limit
        self.unit = unit
        self.reset()

    def reset(self):
        self.last_reset: None | float = None
        self.last_resolved = 0
        self.pending = 0

    def __await__(self):
        loop = asyncio.get_running_loop()

        if self.last_reset is None:
            self.last_reset = loop.time()
            yield None
            return

        normalized = self.normalized
        remaining = normalized - (loop.time() - self.last_reset)
        remaining += (self.pending - self.last_resolved) * normalized
        self.pending += 1

        if remaining < 0:
            self.last_reset = loop.time()
            yield None
            return

        fut = loop.create_future()
        loop.call_later(remaining, lambda: fut.set_result(None))
        yield from fut
        self.last_resolved += 1
        self.last_reset = loop.time()
        return None

    def __copy__(self):
        instance = type(self).__new__(type(self))
        instance.__dict__.update(self.__dict__)
        instance.reset()
        return instance

    @property
    def normalized(self):
        """normalizes frequency to 'second'  and returns interval between calls"""
        value = self.rate_limit
        match self.unit:
            case "second":
                pass
            case "minute":
                value /= 60
            case "hour":
                value /= 3600
            case "day":
                value /= 24 * 3600
            case _:
                raise ValueError(
                    f"Invalid time unit for frequency throtle - should be one of {
                        TIME_UNIT}"
                )
        return 1 / value

    def __repr__(self):
        return f"{self.__class__.__name__}({self.rate_limit}, {self.unit})"


class Stage:
    tasks = None

    def __init__(
        self,
        code,
        *,
        max_concurrency: t.Optional[int] = None,
        rate_limit: None | RateLimiter = None,
        rate_limit_unit: TIME_UNIT = "second",
        preserve_order: bool = True,
        force_concurrency: bool = True,
        parent: "Pipeline" = None,
    ):
        """
        Stage: intended to be used as part of a processing Pipeline

        ...
        force_concurrency: will call synchronous functions using extraasync.sync_to_async in order to paralellize execution
            even for synchronous stage code.
        """
        self.code = code
        self.max_concurrency = max_concurrency
        self.rate_limiter = (
            rate_limit
            if isinstance(rate_limit, RateLimiter)
            else RateLimiter(rate_limit, rate_limit_unit) if rate_limit else None
        )
        self.preserve_order = preserve_order
        self.parent = parent
        self.reset()

    def add_next_stage(self, next_):
        self.next.add(next_)

    def reset(self):
        if self.tasks:
            for task in self.tasks:
                task.cancel()
        self.next = set()
        self.tasks = AutoSet()

    def _collect_result(self, task, next_):
        if not (exc := task.exception()):
            return next_((task.order_tag, task.result()))
        # Hardcoded: exception - ignore.
        logger.error("Exception in pipelined stage: %s", exc)
        self.parent.output.put_nowait((EXC_MARKER, (task.order_tag, exc)))

    def _create_task(self, value: tuple[int, t.Any]):

        order_tag, value = value

        async def rate_limited_async():
            if self.rate_limiter:
                await self.rate_limiter
            return await self.code(value)

        async def rate_limited_sync():
            if self.rate_limiter:
                await self.rate_limiter
            return await async_to_sync(self.code, args=(value,))

        if (
            inspect.iscoroutinefunction(self.code)
            or isinstance(self.code, type)
            and hasattr(self.code, "__await__")
        ):
            # wrapper in all cases because create_task won't work with classes having __await__
            task = asyncio.create_task(rate_limited_async())
        else:
            task = asyncio.create_task(rate_limited_sync())

        task.order_tag = order_tag
        for next_ in self.next:
            task.add_done_callback(partial(self._collect_result, next_=next_))
        task.add_done_callback(self.tasks.remove)
        return task

    def put(self, value: tuple[int, t.Any]):
        # coroutines, awaitable classes:

        if self.max_concurrency in (None, 0) or len(self.tasks) < self.max_concurrency:
            self.tasks.add(self._create_task(value))
        else:
            self.tasks.queue.put_nowait(
                lambda value=value: self._create_task(value))

    def __call__(self, value):
        "just run the stage"
        return self.code(value)

    def __repr__(self):
        return f"{self.__class__.__name__}{self.code}"


class Pipeline:
    """
    Pipeline class
        Will enable mapping data from an iterable source to be passed down various stages
        of execution, where the result of each stage is fed to the next one

        The difference for just calling one (or more) stages inline in a for function
        that pipeline allows for fine grained concurrency specification and error handling

    """

    stages = None

    def __init__(
        self,
        source: t.Optional[t.AsyncIterable[T] | t.Iterable[T]],
        *stages: t.Sequence[t.Callable | Stage],
        max_concurrency: t.Optional[int] = None,
        rate_limit: None | RateLimiter | Real = None,
        rate_limit_unit: TIME_UNIT = "second",
        on_error: PipelineErrors = "eager",
        preserve_order: bool = False,
        max_simultaneous_records: t.Optional[int] = None,
        sink: None | SupportsRShift | MutableSequence | MutableSet = None,
    ):
        """
        Args:
            - stages: One async or sync callable which will process one data item at a time
                - TBD? accept generators as stages? (input data would be ".send"ed into it)
            - data: async or sync generator representing the data source
            - max_concurrency: Maximum number of concurrent tasks _for_ _each_ stage
                (i.e. if there are 2 stages, and max_concurrency is set to 4, we may have
                up to 8 concurrent tasks running at once in the pipeline, but each stage is
                limited to 4)
            - on_error: WHat to do if any stage raises an exception - defaults to re-raise the
                    exception and stop the whole pipeline
            - rate_limit: An overall rate-limitting parameter which can be used to throtle all stages.
                    If anyone stage should have a limit different from the limit to the whole pipeline,
                    create it as an explicit Stage instance and configure the limiter there.
            - rate_limit_unit: if rate_limit is given as a number, this states the time unit to be used in the rate limiting ratio.
                                Not used otherwise.
            - preserve_order: whether to yield the final results in the same order they were acquired from data.
            - max_simultaneous_records: limit on amount of records to hold across all stages and input in internal
                data structures: the idea is throtle data consumption in order to limit the
                amount of memory used by the Pipeline
            - sink: Instance which is loaded with the processed pipeline items if the Pipeline instance itself if awaited.

        """
        self.max_concurrency = max_concurrency
        self.data = (
            _as_async_iterable(source) if source not in (
                None, Placeholder) else None
        )
        self.preserve_order = preserve_order
        # TBD: maybe allow limitting total memory usage instead of elements in the pipeline?
        self.max_simultaneous_records = max_simultaneous_records
        self.on_error = on_error
        self.raw_stages = stages
        self.rate_limiter = (
            rate_limit
            if isinstance(rate_limit, RateLimiter)
            else RateLimiter(rate_limit, rate_limit_unit) if rate_limit else None
        )
        self.sink = sink
        self.reset()

    def _create_stages(self, stages):
        self.stages = self.stages or []
        for stage in stages:
            if not isinstance(stage, Stage):
                stage = Stage(
                    stage,
                    max_concurrency=self.max_concurrency,
                    preserve_order=self.preserve_order,
                    parent=self,
                    rate_limit=copy(self.rate_limiter),
                )
            else:
                stage.parent = self
            self.stages.append(stage)
        for i, stage in enumerate(reversed(self.stages)):
            if i == 0:
                next_stage = self.output.put_nowait
            else:
                next_stage = self.stages[-i].put
            stage.add_next_stage(next_stage)

    def reset(self):
        self.ordered_results = Heap()
        self.output: asyncio.Queue[tuple[int |
                                         EXC_MARKER], t.Any] = asyncio.Queue()
        self._create_stages(self.raw_stages)
        self.count = 0

    def chain_data(self, data_source):
        """concatenates new iterable after current in process items"""

        # TBD

    async def __aiter__(self):
        """Each iteration retrieves the next final result, after passing it trhough all the stages

        NB: calling this a single time will trigger the Pipeline background execution, and
        more than one item can be (or will be) fectched  from source in a single iteration,
        depending on concurrency configuration.

        Over a call, a single final result is produced, but the machinery is started
        and fed up with the data stages.
        """
        order_marker: EXC_MARKER | int
        inputing_data = True
        active_counter = 0
        data_counter = 0
        last_yielded_index = 0
        while inputing_data or active_counter or self.ordered_results:

            if inputing_data:
                try:
                    item = await anext(self.data)
                    active_counter += 1
                    data_counter += 1
                except StopAsyncIteration:
                    inputing_data = False
            if not self.stages:
                active_counter -= 1
                yield item
                continue
            if inputing_data:
                self.stages[0].put((data_counter, item))

            try:
                order_marker, result_data = self.output.get_nowait()
            except asyncio.queues.QueueEmpty:
                pass
            else:
                active_counter -= 1
                if order_marker is EXC_MARKER:
                    if self.on_error == "ignore":
                        await asyncio.sleep(0)
                        if self.preserve_order:
                            self.ordered_results.push((result_data[0], EXC_MARKER))
                        continue
                    elif self.on_error == "strict":
                        raise result_data[1]
                    elif self.on_error == "lazy":
                        raise NotImplementedError(
                            "Lazy error raising in pipeline")
                if not self.preserve_order:
                    yield result_data
                else:
                    self.ordered_results.push((order_marker, result_data))
            if self.ordered_results.peek() == last_yielded_index + 1:
                last_yielded_index, result_data = self.ordered_results.pop()
                if result_data is not EXC_MARKER:
                    yield result_data

            await asyncio.sleep(0)

    def __await__(self):
        """Process the stages for all items in source,
        keep any results in the given pipeline "sink", if any,
        and returns that.

        """

        async def process(self):

            if hasattr(self.sink, "append"):
                sink = self.sink.append
            elif hasattr(self.sink, "add"):
                sink = self.sink.add
            elif hasattr(self.sink, "__rrshift__"):
                sink = self.sink.__rrshift__
            else:
                sink = lambda *args: None

            async for item in self:
                sink(item)

        yield from asyncio.create_task(process(self))
        return self.sink

    async def results(self):
        """Process the stages for all items in source, and returns the outputs of the last stage as a list"""
        return [r async for r in self]

    def __repr__(self):
        return f"<{type(self).__name__}> stages:{self.stages}  bound data: {self.data}, delivered results: {self.count}>"
