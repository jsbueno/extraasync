import asyncio
import warnings

from unittest import mock

import pytest

from extraasync import ExtraTaskGroup



@pytest.mark.asyncio
async def test_extrataskgroup_default_not_abort_works():
    async def worker(n):
        await asyncio.sleep(n/30)
        if n % 3 == 0:
            raise RuntimeError()
        return n

    exception_group_handled = False
    try:
        async with ExtraTaskGroup() as tg:
            tasks = [tg.create_task(worker(i)) for i in range(10)]
    except *RuntimeError as exceptions:
        exception_group_handled = True
        assert len(exceptions.exceptions) == 4  # indexes 0, 3, 6 and 9.

    assert sum(int(task.cancelled()) for task in tasks) == 0
    assert exception_group_handled, "Exception 'RuntimeError' not raised in group"
    assert not any(task.exception() == asyncio.exceptions.CancelledError for task in tasks)
    assert {task.result() for task in tasks if not task.exception()} == {i for i in range(10) if i % 3}


@pytest.mark.asyncio
async def test_extrataskgroup_opt_in_abort():
    async def worker(n):
        await asyncio.sleep(n/30)
        if n % 3 == 0:
            raise RuntimeError()
        return n

    exception_group_handled = False
    try:
        async with ExtraTaskGroup(default_abort=True) as tg:
            tasks = [tg.create_task(worker(i)) for i in range(10)]
    except *RuntimeError as exceptions:
        exception_group_handled = True
        assert len(exceptions.exceptions) == 1
    assert sum(int(task.cancelled()) for task in tasks) == 9

    assert exception_group_handled, "Exception 'RuntimeError' not raised in group"
    assert {task.result() for task in tasks if not task.cancelled() and not task.exception()} == set()


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



