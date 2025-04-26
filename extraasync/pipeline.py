# SPDX-License-Identifier: CC-PDM-1.0
# author:   Martin Jurča, Joao S. O. Bueno
import asyncio
from functools import partial
from logging import getLogger
from inspect import isawaitable
from itertools import chain
from collections.abc import MutableSet

import inspect
import heapq
import queue

from .sync_async_bridge import async_to_sync

import typing as t


logger = getLogger(__name__)

T = t.TypeVar("T")
R = t.TypeVar("R")


# sentinels:
EOD = object()
EXC_MARKER = object()


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

    WHenever an item is removed/discarded, an item is fetched from the queue as
    a task factory - no argument callable: it is called and added to the set.


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
    if isinstance(iterable, t.AsyncIterable):
        return iterable

    async def _sync_to_async_iterable() -> t.AsyncIterable[T]:
        for item in iterable:
            yield item

    return _sync_to_async_iterable()


PipelineErrors = t.Literal["strict", "ignore", "lazy_raise"]


class Stage:
    tasks = None

    def __init__(
        self,
        code,
        max_concurrency: t.Optional[int] = None,
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
        self.parent.output.put_nowait((EXC_MARKER, exc))

    def _create_task(self, value: tuple[int, t.Any]):

        order_tag, value = value
        if (
            inspect.iscoroutinefunction(self.code)
            or isinstance(self.code, type)
            and hasattr(self.code, "__await__")
        ):
            task = asyncio.create_task(self.code(value))
        else:
            task = async_to_sync(self.code, args=(value,))

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
            self.tasks.queue.put_nowait(lambda value=value: self._create_task(value))

    def __call__(self, value):
        "just run the stage"
        return self.code(value)


class Pipeline:
    """
    Pipeline class
        Will enable mapping data from an iterator source to be passed down various stages
        of execution, where the result of each estage is fed to the next one

        The difference for just calling one (or more) stages inline in a for function
        that pipeline allows for fine grained concurrency specification and error handling

    """

    stages = None

    def __init__(
        self,
        *stages: t.Sequence[t.Callable | Stage],
        data: t.Optional[t.AsyncIterable[T] | t.Iterable[T]],
        max_concurrency: t.Optional[int] = None,
        on_error: PipelineErrors = "strict",
        preserve_order: bool = False,
        max_simultaneous_records: t.Optional[int] = None,
    ):
        self.max_concurrency = max_concurrency
        self.data = _as_async_iterable(data)
        self.preserve_order = preserve_order
        # TBD: maybe allow limitting total memory usage instead of elements in the pipeline?
        self.max_simultaneous_records = max_simultaneous_records
        self.on_error = on_error
        self.raw_stages = stages
        self.reset()

    def _create_stages(self, stages):
        self.stages = self.stages or []
        for stage in stages:
            if not isinstance(stage, Stage):
                stage = Stage(
                    stage, self.max_concurrency, self.preserve_order, parent=self
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
        self.output: asyncio.Queue[tuple[int | EXC_MARKER], t.Any] = asyncio.Queue()
        self._create_stages(self.raw_stages)
        self.count = 0

    def chain_data(self, data_source):
        """concatenates new iterable after current in process items"""

        # TBD

    async def __aiter__(self):
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
                        continue
                    elif self.on_error == "strict":
                        raise result_data[1]
                    elif self.on_error == "lazy":
                        raise NotImplementedError("Lazy error raising in pipeline")
                if not self.preserve_order:
                    yield result_data
                else:
                    self.ordered_results.push((order_marker, result_data))
            if self.ordered_results.peek() == last_yielded_index + 1:
                last_yielded_index, result_data = self.ordered_results.pop()
                yield result_data

            await asyncio.sleep(0)

    async def results(self):
        return [r async for r in self]

    def __repr__(self):
        return f"<{type(self).__name__}> stages:{self.stages}  bound data: {self.data}, delivered results: {self.count}>"
