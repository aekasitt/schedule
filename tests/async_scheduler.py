"""Unit tests for async_scheduler.py"""
import datetime
import mock
import pytest
import sys
from typing import Callable

from .schedule import make_mock_job, mock_datetime

if sys.version_info >= (3, 6, 0):
    import schedule
    import asyncio
else:
    raise unittest.SkipTest("AsyncMock is supported since version 3.6")

async_scheduler = schedule.AsyncScheduler()

@pytest.fixture
def setUp():
    async_scheduler.clear()

def make_async_mock_job(name="async_job") -> Callable:
    job = mock.AsyncMock()
    job.__name__ = name
    return job

async def stop_job():
    return schedule.CancelJob

async def increment(array, index):
    array[index] += 1

@pytest.mark.asyncio
async def test_async_sample(setUp):
    duration = 10  # seconds
    test_array = [0] * duration

    for index, value in enumerate(test_array):
        async_scheduler.every(index + 1).seconds.do(increment, test_array, index)

    start = datetime.datetime.now()
    current = start

    while (current - start).total_seconds() < duration:
        await async_scheduler.run_pending()
        await asyncio.sleep(1)
        current = datetime.datetime.now()

    for index, value in enumerate(test_array):
        position = index + 1
        expected = duration / position
        expected = int(expected) if expected != int(expected) else expected - 1
        error_msg = "unexpected value for {}th".format(position)
        assert value == expected, error_msg

@pytest.mark.asyncio
async def test_async_run_pending(setUp):
    mock_job = make_async_mock_job()

    with mock_datetime(2010, 1, 6, 12, 15):
        async_scheduler.every().minute.do(mock_job)
        async_scheduler.every().hour.do(mock_job)
        async_scheduler.every().day.do(mock_job)
        async_scheduler.every().sunday.do(mock_job)
        await async_scheduler.run_pending()
        assert mock_job.call_count == 0

    with mock_datetime(2010, 1, 6, 12, 16):
        await async_scheduler.run_pending()
        assert mock_job.call_count == 1

    with mock_datetime(2010, 1, 6, 13, 16):
        mock_job.reset_mock()
        await async_scheduler.run_pending()
        assert mock_job.call_count == 2

    with mock_datetime(2010, 1, 7, 13, 16):
        mock_job.reset_mock()
        await async_scheduler.run_pending()
        assert mock_job.call_count == 3

    with mock_datetime(2010, 1, 10, 13, 16):
        mock_job.reset_mock()
        await async_scheduler.run_pending()
        assert mock_job.call_count == 4

@pytest.mark.asyncio
async def test_async_run_all(setUp):
    mock_job = make_async_mock_job()
    async_scheduler.every().minute.do(mock_job)
    async_scheduler.every().hour.do(mock_job)
    async_scheduler.every().day.at("11:00").do(mock_job)
    await async_scheduler.run_all()
    assert mock_job.call_count == 3

@pytest.mark.asyncio
async def test_async_job_func_args_are_passed_on(setUp):
    mock_job = make_async_mock_job()
    async_scheduler.every().second.do(mock_job, 1, 2, "three", foo=23, bar={})
    await async_scheduler.run_all()
    mock_job.assert_called_once_with(1, 2, "three", foo=23, bar={})

@pytest.mark.asyncio
async def test_cancel_async_job(setUp):
    mock_job = make_async_mock_job()

    async_scheduler.every().second.do(stop_job)
    mj = async_scheduler.every().second.do(mock_job)
    assert len(async_scheduler.jobs) == 2

    await async_scheduler.run_all()
    assert len(async_scheduler.jobs) == 1
    assert async_scheduler.jobs[0] == mj

    async_scheduler.cancel_job("Not a job")
    assert len(async_scheduler.jobs) == 1

    async_scheduler.cancel_job(mj)
    assert len(async_scheduler.jobs) == 0

@pytest.mark.asyncio
async def test_cancel_async_jobs(setUp):
    async_scheduler.every().second.do(stop_job)
    async_scheduler.every().second.do(stop_job)
    async_scheduler.every().second.do(stop_job)
    assert len(async_scheduler.jobs) == 3

    await async_scheduler.run_all()
    assert len(async_scheduler.jobs) == 0

@pytest.mark.asyncio
async def test_mixed_sync_async_tasks(setUp):
    async_func = make_async_mock_job()
    sync_func = make_mock_job()

    async_scheduler.every().second.do(async_func)
    async_scheduler.every().second.do(sync_func)
    assert async_func.call_count == 0
    assert sync_func.call_count == 0

    await async_scheduler.run_all()
    assert async_func.call_count == 1
    assert sync_func.call_count == 1
