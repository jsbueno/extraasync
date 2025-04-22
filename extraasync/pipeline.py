# SPDX-License-Identifier: CC-PDM-1.0
# author:   Martin Jurča, Joao S. O. Bueno
import asyncio
from inspect import isawaitable
from itertools import chain
from collections.abc import MutableSet

import inspect
import queue

from .sync_async_bridge import async_to_sync

import typing as t

T = t.TypeVar("T")
R = t.TypeVar("R")


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
        self, code, max_concurrency: t.Optional[int] = None, preserve_order: bool = True, force_concurrency: bool = True
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
        self.reset()

    def add_next_stage(self, next_):
        self.next.add(next_)

    def reset(self):
        if self.tasks:
            for task in self.tasks:
                task.cancel()
        self.next = set()
        self.tasks = AutoSet()

    def _create_task(self, value):

        if inspect.iscoroutinefunction(self.code) or isinstance(self.code, type) and hasattr(self.code, "__await__"):
            task = asyncio.create_task(self.code(value))
        else:
            task = async_to_sync(self.code, args=(value,))
        for next_ in self.next:
            # task.add_done_callback(lambda task: (print(task, next_), next_(task.result()) ) )
            task.add_done_callback(lambda task: next_(task.result()))
        task.add_done_callback(self.tasks.remove)
        return task

    def put(self, value, ordering_tag = None):
        # coroutines, awaitable classes:

        if self.max_concurrency in (None, 0) or len(self.tasks) < self.max_concurrency:
            self.tasks.add(self._create_task(value))
        else:
            self.tasks.queue.put_nowait(lambda value=value: self._create_task(value))


    def __call__(self, value):
        "just run the stage"
        return self.code(value)

EOD = object()


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
        preserve_order: bool = True,
    ):
        self.max_concurrency = max_concurrency
        self.data = _as_async_iterable(data)
        self.preserve_order = preserve_order
        self.raw_stages = stages
        self.reset()

    def _create_stages(self, stages):
        self.stages = self.stages or []
        for stage in stages:
            if not isinstance(stage, Stage):
                stage = Stage(stage, self.max_concurrency, self.preserve_order)
            self.stages.append(stage)
        for i, stage in enumerate(reversed(self.stages)):
            if i == 0:
                next_stage = self.output.put_nowait
            else:
                next_stage = self.stages[-i].put
            stage.add_next_stage(next_stage)

    def reset(self):
        self.output = asyncio.Queue()
        self._create_stages(self.raw_stages)
        self.count = 0

    def chain_data(self, data_source):
        """concatenates new iterable after current in process items"""

        # TBD


    async def __aiter__(self):
        inputing_data = True
        data_counter = 0
        while inputing_data or data_counter:

            if inputing_data:
                try:
                    item = await anext(self.data)
                    data_counter += 1
                except StopAsyncIteration:
                    inputing_data = False
            if not self.stages:
                data_counter -= 1
                yield item
                continue
            if inputing_data:
                self.stages[0].put(item)

            try:
                result_data = self.output.get_nowait()
            except asyncio.queues.QueueEmpty:
                pass
            else:
                data_counter -= 1
                yield result_data
            await asyncio.sleep(0)


    async def results(self):
        return [r async for r in self]

    def __repr__(self):
        return f"<{type(self).__name__}> stages:{self.stages}  bound data: {self.data}, delivered results: {self.count}>"
