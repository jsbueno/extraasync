import asyncio
import contextvars
import threading
import time

import pytest

from extraasync import sync_to_async, async_to_sync

def test_plain_sync_to_async():

    done = False
    async def blah():
        nonlocal done
        await asyncio.sleep(0.01)
        done = True
    sync_to_async(blah)
    assert done


def test_plain_sync_to_async_with_coroutine():

    done = False
    async def blah():
        nonlocal done
        await asyncio.sleep(0.01)
        done = True
    sync_to_async(blah())  # <- pass a co-routine directly
    assert done



def test_async_sync_plain():
    # this is actually a no-op,
    # but for the fact the sync function
    # now has to be awaited.

    done = False

    def blah():
        nonlocal done
        done = True

    async def bleh():
        await async_to_sync(blah)
    asyncio.run(bleh())

    assert done


def test_async_sync_bridge_1():
    # Debugging prints and helpers
    # left on purpose. They might be needded
    # when developping new features

    done = done2 = done3 = False

    # Useful for debugging
    #async def heartbeat():
        #counter = 0
        #while True:
            #await asyncio.sleep(1)
            #print(counter, asyncio.all_tasks())
            #counter += 1

    async def blah():
        nonlocal done
        #print("@" * 100)
        await asyncio.sleep(0.01)
        done = True

    def bleh():
        nonlocal done2
        #print("#" * 100)
        sync_to_async(blah)
        done2 = True

    async def blih():
        nonlocal done3
        #t = asyncio.create_task(heartbeat())
        #print("*" * 100)
        await async_to_sync(bleh)
        #t.cancel()
        done3 = True

    asyncio.run(blih())
    assert done and done2 and done3


def test_async_sync_bridge_2():
    done = done2 = done3 = False

    async def blah():
        nonlocal done
        await asyncio.sleep(0.01)
        done = True

    def bleh():
        nonlocal done2
        sync_to_async(blah)
        done2 = True

    async def blih():
        nonlocal done3
        await async_to_sync(bleh)
        done3 = True

    sync_to_async(blih)
    assert done and done2 and done3


def test_async_sync_bridge_return_values():

    async def blah():
        await asyncio.sleep(0.01)
        return 23

    def bleh():
        return sync_to_async(blah)

    async def blih():
        return await async_to_sync(bleh)

    assert sync_to_async(blih) == 23


def test_async_sync_bridge_can_pass_positional_args():

    async def blah(value):
        await asyncio.sleep(0.01)
        return value

    def bleh(value):
        return sync_to_async(blah, args=(value,))

    async def blih(value):
        return await async_to_sync(bleh, args=(value,))

    assert sync_to_async(blih, args=(23,)) == 23


def test_async_sync_bridge_can_pass_named_args():

    async def blah(*, value):
        await asyncio.sleep(0.01)
        return value

    def bleh(*, value):
        return sync_to_async(blah, kwargs={"value": value})

    async def blih(*, value):
        return await async_to_sync(bleh, kwargs={"value": value})

    assert sync_to_async(blih, kwargs={"value": 23}) == 23


def test_async_sync_bridge_depth2_chain_call():

    done = done2 = done3 = done4 = done5 = False

    async def blah():
        nonlocal done
        await asyncio.sleep(0.01)
        done = True

    def bleh():
        nonlocal done2
        sync_to_async(blah)
        done2 = True

    async def blih():
        nonlocal done3
        await async_to_sync(bleh)
        done3 = True

    def bloh():
        nonlocal done4
        sync_to_async(blih)
        done4 = True

    async def bluh():
        nonlocal done5
        await async_to_sync(bloh)
        done5 = True

    asyncio.run(bluh())
    assert done and done2 and done3 and done4 and done5


@pytest.mark.parametrize(["depth"], [
  (5,),
  (10,),
  (20,),
  #(5000,),  # try it. You know you want to!
])
def test_async_sync_bridge_depth_arbitrary(depth):

    steps_done = 0

    async def async_target(depth):
        nonlocal steps_done
        # print("\nplim", depth)
        if depth == 0:
            await asyncio.sleep(0.01)
        else:
            await async_to_sync(sync_target, args=((depth - 1),))
        steps_done += 1

    def sync_target(depth):
        nonlocal steps_done
        # print("\nplom", depth)
        if depth:
            sync_to_async(async_target(depth - 1))
        steps_done += 1


    asyncio.run(async_target(depth - 1))
    assert steps_done == depth


def test_async_sync_bridge_stops_on_error():
    # During development, a frequent error
    # was having the executiong hanging when an
    # inner sync_to_async call errored.

    done = done2 = done3 = False
    stalled = False

    def bad_async_target():  # not a co-routine: should raise an inner exception
        nonlocal done
        done = True

    def sync_step():
        nonlocal done2
        sync_to_async(bad_async_target)
        done2 = True

    async def async_entry():
        nonlocal done3, stalled
        try:
            await asyncio.wait_for(async_to_sync(sync_step), timeout=0.05)
        except TimeoutError:
            stalled = "Async bridge call stalled on bad-awaitable passed: check 'do_it' nested function in 'sinc_to_async' call."
        except TypeError:
            done3 = True

    asyncio.run(async_entry())
    assert not stalled, stalled
    assert done and not done2 and done3


def test_async_sync_bridged_tasks_run_on_same_loop_and_same_thread():
    done = done2 = done3 = False

    inner_thread  = outer_thread = None
    inner_loop = outer_loop = None

    async def blah():
        nonlocal done, inner_thread, inner_loop
        inner_thread = threading.current_thread()
        inner_loop = asyncio.get_running_loop()
        await asyncio.sleep(0.01)
        done = True

    def bleh():
        nonlocal done2
        sync_to_async(blah)
        done2 = True

    async def blih():
        nonlocal done3, outer_thread, outer_loop
        await async_to_sync(bleh)

        outer_thread = threading.current_thread()
        outer_loop = asyncio.get_running_loop()

        done3 = True

    sync_to_async(blih)
    assert done and done2 and done3
    assert inner_thread is outer_thread
    assert inner_loop is outer_loop


def test_async_sync_concurrent_sync_tasks_run_on_different_threads():
    n = 5

    used_threads = set()
    results = None

    def sync_call():
        time.sleep(0.02)
        used_threads.add(threading.current_thread())
        return 23

    async def async_main():
        nonlocal results
        tasks = set()
        for i in range(n):
            tasks.add(async_to_sync(sync_call))
        results = tuple(await asyncio.gather(*tasks))

    sync_to_async(async_main)
    assert results == (23,) * n
    assert len(used_threads) == n


def test_async_sync_sequential_sync_tasks_run_reuse_threads():
    n = 5

    used_threads = set()
    results = None

    def sync_call():
        time.sleep(0.002)
        used_threads.add(threading.current_thread())
        return 23

    async def async_main():
        nonlocal results
        tasks = set()
        results = []
        for i in range(n):
            results.append(await async_to_sync(sync_call))

    sync_to_async(async_main)
    assert results == [23] * n
    assert len(used_threads) == 1


def test_async_sync_bridge_exceptions_bubble_out():
    done = False

    async def blah():
        nonlocal done
        await asyncio.sleep(0.01)
        done = True
        raise RuntimeError()

    def bleh():
        sync_to_async(blah)

    async def blih():
        await async_to_sync(bleh)

    with pytest.raises(RuntimeError):
        sync_to_async(blih)

    assert done



context_test = contextvars.ContextVar("context_test")

def test_async_sync_bridge_receives_copy_of_the_calling_context():

    context_matches_value = True

    async def async_end(value):
        await asyncio.sleep(0.01)
        if value == context_test.get():
            return value
        return (value, context_test.get())

    def sync_step(value):
        nonlocal context_matches_value
        if value != context_test.get():
            context_matches_value = False
        return sync_to_async(async_end, args=(value,))

    async def trampoline(value):
        context_test.set(value)
        return await async_to_sync(sync_step, args=(value,))

    async def async_entry():
        return set(await asyncio.gather(*(trampoline(value) for value in (5, 23, 55, 3125))))


    assert sync_to_async(async_entry) == {5, 23, 55, 3125}


def test_async_sync_bridge_sync_steps_can_modify_context():

    context_matches_value = True

    async def async_end(value):
        await asyncio.sleep(0.01)
        if value == context_test.get():
            return value
        return (value, context_test.get())

    def sync_step(value):

        # This is what we are testing here:
        # sync call, in other thread, sets context value visible back on async task!
        context_test.set(value)

        # delay to ensure calls take place in parallel.
        time.sleep(0.02)
        return sync_to_async(async_end, args=(value,))

    async def trampoline(value):
        context_test.set(value / 100)   # This should not be visible in other async end.
        return await async_to_sync(sync_step, args=(value,))

    async def async_entry():
        return set(await asyncio.gather(*(trampoline(value) for value in (5, 23, 55, 3125))))


    assert sync_to_async(async_entry) == {5, 23, 55, 3125}
