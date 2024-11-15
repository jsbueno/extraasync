import asyncio
import warnings

from unittest import mock

import pytest

from extraasync import ExtraTaskGroup



@pytest.mark.asyncio
async def test_extrataskgroup_works():
    async def worker(n):
        await asyncio.sleep(n/30)
        if n % 3 == 0:
            raise RuntimeError()
        return n

    try:
        async with ExtraTaskGroup() as tg:
            tasks = [tg.create_task(worker(i)) for i in range(10)]
    except *RuntimeError as exceptions:
        assert len(exceptions.exceptions) == 4

    assert not any(task.exception() == asyncio.exceptions.CancelledError for task in tasks)
    assert {task.result() for task in tasks if not task.exception()} == {i for i in range(10) if i % 3}


def test_package_warns_if_taskgroup_implementation_becomes_incompatible():
    import sys
    del sys.modules["extraasync.taskgroup"]
    del sys.modules["asyncio.taskgroups"]
    class Dummy:
        pass

    with mock.patch("asyncio.taskgroups.TaskGroup", Dummy), mock.patch("asyncio.TaskGroup", Dummy):
        with pytest.warns():
            from extraasync.taskgroup import ExtraTaskGroup
    import asyncio.taskgroups


