"""Unit tests for schedule.py"""
import datetime
import functools
import mock
import pytest
from typing import Callable

import schedule
from schedule import IntervalError, ScheduleError, ScheduleValueError, every, repeat


# Silence "missing docstring", "method could be a function",
# "class already defined", and "too many public methods" messages:
# pylint: disable-msg=R0201,C0111,E0102,R0904,R0901


@pytest.fixture
def mock_job(name=None) -> Callable:
    job = mock.Mock()
    job.__name__ = name or "job"
    return job


@pytest.fixture
@pytest.mark.parametrize("mock_job", ["hourly"])
def mock_hourly_job(mock_job):
    return mock_job


@pytest.fixture
@pytest.mark.parametrize("mock_job", ["daily"])
def mock_daily_job(mock_job):
    return mock_job


@pytest.fixture
@pytest.mark.parametrize("mock_job", ["job1"])
def mock_job1(mock_job):
    return mock_job


@pytest.fixture
@pytest.mark.parametrize("mock_job", ["job2"])
def mock_job2(mock_job):
    return mock_job


@pytest.fixture
@pytest.mark.parametrize("mock_job", ["job3"])
def mock_job3(mock_job):
    return mock_job


class mock_datetime(object):
    """
    Monkey-patch datetime for predictable results
    """

    def __init__(self, year, month, day, hour, minute, second=0):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute
        self.second = second

    def __enter__(self):
        class MockDate(datetime.datetime):
            @classmethod
            def today(cls):
                return cls(self.year, self.month, self.day)

            @classmethod
            def now(cls):
                return cls(
                    self.year,
                    self.month,
                    self.day,
                    self.hour,
                    self.minute,
                    self.second,
                )

        self.original_datetime = datetime.datetime
        datetime.datetime = MockDate

        return MockDate(
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )

    def __exit__(self, *args, **kwargs):
        datetime.datetime = self.original_datetime


@pytest.fixture
def set_up():
    schedule.clear()


def test_time_units(set_up):
    assert every().seconds.unit == "seconds"
    assert every().minutes.unit == "minutes"
    assert every().hours.unit == "hours"
    assert every().days.unit == "days"
    assert every().weeks.unit == "weeks"

    job_instance = schedule.Job(interval=2)
    # without a context manager, it incorrectly raises an error because
    # it is not callable
    with pytest.raises(IntervalError):
        job_instance.minute
    with pytest.raises(IntervalError):
        job_instance.hour
    with pytest.raises(IntervalError):
        job_instance.day
    with pytest.raises(IntervalError):
        job_instance.week
    with pytest.raises(IntervalError) as exc:
        job_instance.monday
    assert exc.match(
        r"Scheduling \.monday\(\) jobs is only allowed for weekly jobs\. "
        r"Using \.monday\(\) on a job scheduled to run every 2 or more "
        r"weeks is not supported\."
    )

    with pytest.raises(IntervalError) as exc:
        job_instance.tuesday
    assert exc.match(
        r"Scheduling \.tuesday\(\) jobs is only allowed for weekly jobs\. "
        r"Using \.tuesday\(\) on a job scheduled to run every 2 or more "
        r"weeks is not supported\."
    )

    with pytest.raises(IntervalError) as exc:
        job_instance.wednesday
    assert exc.match(
        r"Scheduling \.wednesday\(\) jobs is only allowed for weekly jobs\. "
        r"Using \.wednesday\(\) on a job scheduled to run every 2 or more "
        r"weeks is not supported\."
    )

    with pytest.raises(IntervalError) as exc:
        job_instance.thursday
    assert exc.match(
        r"Scheduling \.thursday\(\) jobs is only allowed for weekly jobs\. "
        r"Using \.thursday\(\) on a job scheduled to run every 2 or more "
        r"weeks is not supported\."
    )

    with pytest.raises(IntervalError) as exc:
        job_instance.friday
    assert exc.match(
        r"Scheduling \.friday\(\) jobs is only allowed for weekly jobs\. "
        r"Using \.friday\(\) on a job scheduled to run every 2 or more "
        r"weeks is not supported\."
    )

    with pytest.raises(IntervalError) as exc:
        job_instance.saturday
    assert exc.match(
        r"Scheduling \.saturday\(\) jobs is only allowed for weekly jobs\. "
        r"Using \.saturday\(\) on a job scheduled to run every 2 or more "
        r"weeks is not supported\."
    )

    with pytest.raises(IntervalError) as exc:
        job_instance.sunday
    assert exc.match(
        r"Scheduling \.sunday\(\) jobs is only allowed for weekly jobs\. "
        r"Using \.sunday\(\) on a job scheduled to run every 2 or more "
        r"weeks is not supported\."
    )

    # test an invalid unit
    job_instance.unit = "foo"
    pytest.raises(ScheduleValueError, job_instance.at, "1:0:0")
    pytest.raises(ScheduleValueError, job_instance._schedule_next_run)

    # test start day exists but unit is not 'weeks'
    job_instance.unit = "days"
    job_instance.start_day = 1
    pytest.raises(ScheduleValueError, job_instance._schedule_next_run)

    # test weeks with an invalid start day
    job_instance.unit = "weeks"
    job_instance.start_day = "bar"
    pytest.raises(ScheduleValueError, job_instance._schedule_next_run)

    # test a valid unit with invalid hours/minutes/seconds
    job_instance.unit = "days"
    pytest.raises(ScheduleValueError, job_instance.at, "25:00:00")
    pytest.raises(ScheduleValueError, job_instance.at, "00:61:00")
    pytest.raises(ScheduleValueError, job_instance.at, "00:00:61")

    # test invalid time format
    pytest.raises(ScheduleValueError, job_instance.at, "25:0:0")
    pytest.raises(ScheduleValueError, job_instance.at, "0:61:0")
    pytest.raises(ScheduleValueError, job_instance.at, "0:0:61")

    # test (very specific) seconds with unspecified start_day
    job_instance.unit = "seconds"
    job_instance.at_time = datetime.datetime.now()
    job_instance.start_day = None
    pytest.raises(ScheduleValueError, job_instance._schedule_next_run)

    # test set_up.latest >= set_up.interval
    job_instance.latest = 1
    pytest.raises(ScheduleError, job_instance._schedule_next_run)
    job_instance.latest = 3
    pytest.raises(ScheduleError, job_instance._schedule_next_run)


def test_singular_time_units_match_plural_units(set_up):
    assert every().second.unit == every().seconds.unit
    assert every().minute.unit == every().minutes.unit
    assert every().hour.unit == every().hours.unit
    assert every().day.unit == every().days.unit
    assert every().week.unit == every().weeks.unit


def test_time_range(set_up, mock_job):
    with mock_datetime(2014, 6, 28, 12, 0):
        # Choose a sample size large enough that it's unlikely the
        # same value will be chosen each time.
        minutes = set(
            [every(5).to(30).minutes.do(mock_job).next_run.minute for i in range(100)]
        )

        assert len(minutes) > 1
        assert min(minutes) >= 5
        assert max(minutes) <= 30


def test_time_range_repr(set_up, mock_job):

    with mock_datetime(2014, 6, 28, 12, 0):
        job_repr = repr(every(5).to(30).minutes.do(mock_job))

    assert job_repr.startswith("Every 5 to 30 minutes do job()")


def test_at_time(set_up, mock_job):
    assert every().day.at("10:30").do(mock_job).next_run.hour == 10
    assert every().day.at("10:30").do(mock_job).next_run.minute == 30
    assert every().day.at("10:30:50").do(mock_job).next_run.second == 50

    pytest.raises(ScheduleValueError, every().day.at, "2:30:000001")
    pytest.raises(ScheduleValueError, every().day.at, "::2")
    pytest.raises(ScheduleValueError, every().day.at, ".2")
    pytest.raises(ScheduleValueError, every().day.at, "2")
    pytest.raises(ScheduleValueError, every().day.at, ":2")
    pytest.raises(ScheduleValueError, every().day.at, " 2:30:00")
    pytest.raises(ScheduleValueError, every().do, lambda: 0)
    pytest.raises(TypeError, every().day.at, 2)

    # without a context manager, it incorrectly raises an error because
    # it is not callable
    with pytest.raises(IntervalError):
        every(interval=2).second
    with pytest.raises(IntervalError):
        every(interval=2).minute
    with pytest.raises(IntervalError):
        every(interval=2).hour
    with pytest.raises(IntervalError):
        every(interval=2).day
    with pytest.raises(IntervalError):
        every(interval=2).week
    with pytest.raises(IntervalError):
        every(interval=2).monday
    with pytest.raises(IntervalError):
        every(interval=2).tuesday
    with pytest.raises(IntervalError):
        every(interval=2).wednesday
    with pytest.raises(IntervalError):
        every(interval=2).thursday
    with pytest.raises(IntervalError):
        every(interval=2).friday
    with pytest.raises(IntervalError):
        every(interval=2).saturday
    with pytest.raises(IntervalError):
        every(interval=2).sunday


def test_until_time(set_up, mock_job):
    # Check argument parsing
    with mock_datetime(2020, 1, 1, 10, 0, 0) as m:
        assert every().day.until(datetime.datetime(3000, 1, 1, 20, 30)).do(
            mock_job
        ).cancel_after == datetime.datetime(3000, 1, 1, 20, 30, 0)
        assert every().day.until(datetime.datetime(3000, 1, 1, 20, 30, 50)).do(
            mock_job
        ).cancel_after == datetime.datetime(3000, 1, 1, 20, 30, 50)
        assert every().day.until(datetime.time(12, 30)).do(
            mock_job
        ).cancel_after == m.replace(hour=12, minute=30, second=0, microsecond=0)
        assert every().day.until(datetime.time(12, 30, 50)).do(
            mock_job
        ).cancel_after == m.replace(hour=12, minute=30, second=50, microsecond=0)

        assert every().day.until(
            datetime.timedelta(days=40, hours=5, minutes=12, seconds=42)
        ).do(mock_job).cancel_after == datetime.datetime(2020, 2, 10, 15, 12, 42)

        assert every().day.until("10:30").do(mock_job).cancel_after == m.replace(
            hour=10, minute=30, second=0, microsecond=0
        )
        assert every().day.until("10:30:50").do(mock_job).cancel_after == m.replace(
            hour=10, minute=30, second=50, microsecond=0
        )
        assert every().day.until("3000-01-01 10:30").do(
            mock_job
        ).cancel_after == datetime.datetime(3000, 1, 1, 10, 30, 0)
        assert every().day.until("3000-01-01 10:30:50").do(
            mock_job
        ).cancel_after == datetime.datetime(3000, 1, 1, 10, 30, 50)
        assert every().day.until(datetime.datetime(3000, 1, 1, 10, 30, 50)).do(
            mock_job
        ).cancel_after == datetime.datetime(3000, 1, 1, 10, 30, 50)

    # Invalid argument types
    pytest.raises(TypeError, every().day.until, 123)
    pytest.raises(ScheduleValueError, every().day.until, "123")
    pytest.raises(ScheduleValueError, every().day.until, "01-01-3000")

    # Using .until() with moments in the passed
    pytest.raises(
        ScheduleValueError,
        every().day.until,
        datetime.datetime(2019, 12, 31, 23, 59),
    )
    pytest.raises(ScheduleValueError, every().day.until, datetime.timedelta(minutes=-1))
    pytest.raises(ScheduleValueError, every().day.until, datetime.time(hour=5))

    # Unschedule job after next_run passes the deadline
    schedule.clear()
    with mock_datetime(2020, 1, 1, 11, 35, 10):
        mock_job.reset_mock()
        every(5).seconds.until(datetime.time(11, 35, 20)).do(mock_job)
        with mock_datetime(2020, 1, 1, 11, 35, 15):
            schedule.run_pending()
            assert mock_job.call_count == 1
            assert len(schedule.jobs) == 1
        with mock_datetime(2020, 1, 1, 11, 35, 20):
            schedule.run_all()
            assert mock_job.call_count == 2
            assert len(schedule.jobs) == 0

    # Unschedule job because current execution time has passed deadline
    schedule.clear()
    with mock_datetime(2020, 1, 1, 11, 35, 10):
        mock_job.reset_mock()
        every(5).seconds.until(datetime.time(11, 35, 20)).do(mock_job)
        with mock_datetime(2020, 1, 1, 11, 35, 50):
            schedule.run_pending()
            assert mock_job.call_count == 0
            assert len(schedule.jobs) == 0


def test_weekday_at_todady(set_up, mock_job):

    # This date is a wednesday
    with mock_datetime(2020, 11, 25, 22, 38, 5):
        job = every().wednesday.at("22:38:10").do(mock_job)
        assert job.next_run.hour == 22
        assert job.next_run.minute == 38
        assert job.next_run.second == 10
        assert job.next_run.year == 2020
        assert job.next_run.month == 11
        assert job.next_run.day == 25

        job = every().wednesday.at("22:39").do(mock_job)
        assert job.next_run.hour == 22
        assert job.next_run.minute == 39
        assert job.next_run.second == 00
        assert job.next_run.year == 2020
        assert job.next_run.month == 11
        assert job.next_run.day == 25


def test_at_time_hour(set_up, mock_job):
    with mock_datetime(2010, 1, 6, 12, 20):
        assert every().hour.at(":30").do(mock_job).next_run.hour == 12
        assert every().hour.at(":30").do(mock_job).next_run.minute == 30
        assert every().hour.at(":30").do(mock_job).next_run.second == 0
        assert every().hour.at(":10").do(mock_job).next_run.hour == 13
        assert every().hour.at(":10").do(mock_job).next_run.minute == 10
        assert every().hour.at(":10").do(mock_job).next_run.second == 0
        assert every().hour.at(":00").do(mock_job).next_run.hour == 13
        assert every().hour.at(":00").do(mock_job).next_run.minute == 0
        assert every().hour.at(":00").do(mock_job).next_run.second == 0

        pytest.raises(ScheduleValueError, every().hour.at, "2:30:00")
        pytest.raises(ScheduleValueError, every().hour.at, "::2")
        pytest.raises(ScheduleValueError, every().hour.at, ".2")
        pytest.raises(ScheduleValueError, every().hour.at, "2")
        pytest.raises(ScheduleValueError, every().hour.at, " 2:30")
        pytest.raises(ScheduleValueError, every().hour.at, "61:00")
        pytest.raises(ScheduleValueError, every().hour.at, "00:61")
        pytest.raises(ScheduleValueError, every().hour.at, "01:61")
        pytest.raises(TypeError, every().hour.at, 2)

        # test the 'MM:SS' format
        assert every().hour.at("30:05").do(mock_job).next_run.hour == 12
        assert every().hour.at("30:05").do(mock_job).next_run.minute == 30
        assert every().hour.at("30:05").do(mock_job).next_run.second == 5
        assert every().hour.at("10:25").do(mock_job).next_run.hour == 13
        assert every().hour.at("10:25").do(mock_job).next_run.minute == 10
        assert every().hour.at("10:25").do(mock_job).next_run.second == 25
        assert every().hour.at("00:40").do(mock_job).next_run.hour == 13
        assert every().hour.at("00:40").do(mock_job).next_run.minute == 0
        assert every().hour.at("00:40").do(mock_job).next_run.second == 40


def test_at_time_minute(set_up, mock_job):
    with mock_datetime(2010, 1, 6, 12, 20, 30):
        assert every().minute.at(":40").do(mock_job).next_run.hour == 12
        assert every().minute.at(":40").do(mock_job).next_run.minute == 20
        assert every().minute.at(":40").do(mock_job).next_run.second == 40
        assert every().minute.at(":10").do(mock_job).next_run.hour == 12
        assert every().minute.at(":10").do(mock_job).next_run.minute == 21
        assert every().minute.at(":10").do(mock_job).next_run.second == 10

        pytest.raises(ScheduleValueError, every().minute.at, "::2")
        pytest.raises(ScheduleValueError, every().minute.at, ".2")
        pytest.raises(ScheduleValueError, every().minute.at, "2")
        pytest.raises(ScheduleValueError, every().minute.at, "2:30:00")
        pytest.raises(ScheduleValueError, every().minute.at, "2:30")
        pytest.raises(ScheduleValueError, every().minute.at, " :30")
        pytest.raises(TypeError, every().minute.at, 2)


def test_next_run_time(set_up, mock_job):
    with mock_datetime(2010, 1, 6, 12, 15):
        assert schedule.next_run() is None
        assert every().minute.do(mock_job).next_run.minute == 16
        assert every(5).minutes.do(mock_job).next_run.minute == 20
        assert every().hour.do(mock_job).next_run.hour == 13
        assert every().day.do(mock_job).next_run.day == 7
        assert every().day.at("09:00").do(mock_job).next_run.day == 7
        assert every().day.at("12:30").do(mock_job).next_run.day == 6
        assert every().week.do(mock_job).next_run.day == 13
        assert every().monday.do(mock_job).next_run.day == 11
        assert every().tuesday.do(mock_job).next_run.day == 12
        assert every().wednesday.do(mock_job).next_run.day == 13
        assert every().thursday.do(mock_job).next_run.day == 7
        assert every().friday.do(mock_job).next_run.day == 8
        assert every().saturday.do(mock_job).next_run.day == 9
        assert every().sunday.do(mock_job).next_run.day == 10
        assert (
            every().minute.until(datetime.time(12, 17)).do(mock_job).next_run.minute
            == 16
        )


def test_next_run_time_day_end(set_up, mock_job):
    # At day 1, schedule job to run at daily 23:30
    with mock_datetime(2010, 12, 1, 23, 0, 0):
        job = every().day.at("23:30").do(mock_job)
        # first occurrence same day
        assert job.next_run.day == 1
        assert job.next_run.hour == 23

    # Running the job 01:00 on day 2, afterwards the job should be
    # scheduled at 23:30 the same day. This simulates a job that started
    # on day 1 at 23:30 and took 1,5 hours to finish
    with mock_datetime(2010, 12, 2, 1, 0, 0):
        job.run()
        assert job.next_run.day == 2
        assert job.next_run.hour == 23

    # Run the job at 23:30 on day 2, afterwards the job should be
    # scheduled at 23:30 the next day
    with mock_datetime(2010, 12, 2, 23, 30, 0):
        job.run()
        assert job.next_run.day == 3
        assert job.next_run.hour == 23


def test_next_run_time_hour_end(set_up, mock_job):
    with mock_datetime(2010, 10, 10, 12, 0, 0):
        job = every().hour.at(":10").do(mock_job)
        assert job.next_run.hour == 12
        assert job.next_run.minute == 10

    with mock_datetime(2010, 10, 10, 13, 0, 0):
        job.run()
        assert job.next_run.hour == 13
        assert job.next_run.minute == 10

    with mock_datetime(2010, 10, 10, 13, 15, 0):
        job.run()
        assert job.next_run.hour == 14
        assert job.next_run.minute == 10


def test_next_run_time_minute_end(set_up, mock_job):
    with mock_datetime(2010, 10, 10, 10, 10, 0):
        job = every().minute.at(":15").do(mock_job)
        assert job.next_run.minute == 10
        assert job.next_run.second == 15

    with mock_datetime(2010, 10, 10, 10, 10, 59):
        job.run()
        assert job.next_run.minute == 11
        assert job.next_run.second == 15

    with mock_datetime(2010, 10, 10, 10, 12, 14):
        job.run()
        assert job.next_run.minute == 12
        assert job.next_run.second == 15

    with mock_datetime(2010, 10, 10, 10, 12, 16):
        job.run()
        assert job.next_run.minute == 13
        assert job.next_run.second == 15


def test_run_all(set_up, mock_job):
    every().minute.do(mock_job)
    every().hour.do(mock_job)
    every().day.at("11:00").do(mock_job)
    schedule.run_all()
    assert mock_job.call_count == 3


def test_run_all_with_decorator(set_up, mock_job):
    @repeat(every().minute)
    def job1():
        mock_job()

    @repeat(every().hour)
    def job2():
        mock_job()

    @repeat(every().day.at("11:00"))
    def job3():
        mock_job()

    schedule.run_all()
    assert mock_job.call_count == 3


def test_run_all_with_decorator_args(set_up, mock_job):
    @repeat(every().minute, 1, 2, "three", foo=23, bar={})
    def job(*args, **kwargs):
        mock_job(*args, **kwargs)

    schedule.run_all()
    mock_job.assert_called_once_with(1, 2, "three", foo=23, bar={})


def test_run_all_with_decorator_defaultargs(set_up, mock_job):
    @repeat(every().minute)
    def job(nothing=None):
        mock_job(nothing)

    schedule.run_all()
    mock_job.assert_called_once_with(None)


def test_job_func_args_are_passed_on(set_up, mock_job):
    every().second.do(mock_job, 1, 2, "three", foo=23, bar={})
    schedule.run_all()
    mock_job.assert_called_once_with(1, 2, "three", foo=23, bar={})


def test_to_string(set_up):
    def job_fun():
        pass

    s = str(every().minute.do(job_fun, "foo", bar=23))
    assert s == (
        "Job(interval=1, unit=minutes, do=job_fun, "
        "args=('foo',), kwargs={'bar': 23})"
    )
    assert "job_fun" in s
    assert "foo" in s
    assert "{'bar': 23}" in s


def test_to_repr(set_up):
    def job_fun():
        pass

    s = repr(every().minute.do(job_fun, "foo", bar=23))
    assert s.startswith(
        "Every 1 minute do job_fun('foo', bar=23) (last run: [never], next run: "
    )
    assert "job_fun" in s
    assert "foo" in s
    assert "bar=23" in s

    # test repr when at_time is not None
    s2 = repr(every().day.at("00:00").do(job_fun, "foo", bar=23))
    assert s2.startswith(
        (
            "Every 1 day at 00:00:00 do job_fun('foo', "
            "bar=23) (last run: [never], next run: "
        )
    )


def test_to_string_lambda_job_func(set_up):
    assert len(str(every().minute.do(lambda: 1))) > 1
    assert len(str(every().day.at("10:30").do(lambda: 1))) > 1


def test_repr_functools_partial_job_func(set_up):
    def job_fun(arg):
        pass

    job_fun = functools.partial(job_fun, "foo")
    job_repr = repr(every().minute.do(job_fun, bar=True, somekey=23))
    assert "functools.partial" in job_repr
    assert "bar=True" in job_repr
    assert "somekey=23" in job_repr


def test_to_string_functools_partial_job_func(set_up):
    def job_fun(arg):
        pass

    job_fun = functools.partial(job_fun, "foo")
    job_str = str(every().minute.do(job_fun, bar=True, somekey=23))
    assert "functools.partial" in job_str
    assert "bar=True" in job_str
    assert "somekey=23" in job_str


def test_run_pending(set_up, mock_job):
    """Check that run_pending() runs pending jobs.
    We do this by overriding datetime.datetime with mock objects
    that represent increasing system times.

    Please note that it is *intended behavior that run_pending() does not
    run missed jobs*. For example, if you've registered a job that
    should run every minute and you only call run_pending() in one hour
    increments then your job won't be run 60 times in between but
    only once.
    """

    with mock_datetime(2010, 1, 6, 12, 15):
        every().minute.do(mock_job)
        every().hour.do(mock_job)
        every().day.do(mock_job)
        every().sunday.do(mock_job)
        schedule.run_pending()
        assert mock_job.call_count == 0

    with mock_datetime(2010, 1, 6, 12, 16):
        schedule.run_pending()
        assert mock_job.call_count == 1

    with mock_datetime(2010, 1, 6, 13, 16):
        mock_job.reset_mock()
        schedule.run_pending()
        assert mock_job.call_count == 2

    with mock_datetime(2010, 1, 7, 13, 16):
        mock_job.reset_mock()
        schedule.run_pending()
        assert mock_job.call_count == 3

    with mock_datetime(2010, 1, 10, 13, 16):
        mock_job.reset_mock()
        schedule.run_pending()
        assert mock_job.call_count == 4


def test_run_every_weekday_at_specific_time_today(set_up, mock_job):
    with mock_datetime(2010, 1, 6, 13, 16):
        every().wednesday.at("14:12").do(mock_job)
        schedule.run_pending()
        assert mock_job.call_count == 0

    with mock_datetime(2010, 1, 6, 14, 16):
        schedule.run_pending()
        assert mock_job.call_count == 1


def test_run_every_weekday_at_specific_time_past_today(set_up, mock_job):
    with mock_datetime(2010, 1, 6, 13, 16):
        every().wednesday.at("13:15").do(mock_job)
        schedule.run_pending()
        assert mock_job.call_count == 0

    with mock_datetime(2010, 1, 13, 13, 14):
        schedule.run_pending()
        assert mock_job.call_count == 0

    with mock_datetime(2010, 1, 13, 13, 16):
        schedule.run_pending()
        assert mock_job.call_count == 1


def test_run_every_n_days_at_specific_time(set_up, mock_job):
    with mock_datetime(2010, 1, 6, 11, 29):
        every(2).days.at("11:30").do(mock_job)
        schedule.run_pending()
        assert mock_job.call_count == 0

    with mock_datetime(2010, 1, 6, 11, 31):
        schedule.run_pending()
        assert mock_job.call_count == 0

    with mock_datetime(2010, 1, 7, 11, 31):
        schedule.run_pending()
        assert mock_job.call_count == 0

    with mock_datetime(2010, 1, 8, 11, 29):
        schedule.run_pending()
        assert mock_job.call_count == 0

    with mock_datetime(2010, 1, 8, 11, 31):
        schedule.run_pending()
        assert mock_job.call_count == 1

    with mock_datetime(2010, 1, 10, 11, 31):
        schedule.run_pending()
        assert mock_job.call_count == 2


def test_next_run_property(set_up, mock_hourly_job, mock_daily_job):
    original_datetime = datetime.datetime
    with mock_datetime(2010, 1, 6, 13, 16):
        hourly_job = mock_hourly_job
        daily_job = mock_daily_job
        every().day.do(daily_job)
        every().hour.do(hourly_job)
        assert len(schedule.jobs) == 2
        # Make sure the hourly job is first
        assert schedule.next_run() == original_datetime(2010, 1, 6, 14, 16)


def test_idle_seconds(set_up, mock_job):
    assert schedule.next_run() is None
    assert schedule.idle_seconds() is None

    with mock_datetime(2020, 12, 9, 21, 46):
        job = every().hour.do(mock_job)
        assert schedule.idle_seconds() == 60 * 60
        schedule.cancel_job(job)
        assert schedule.next_run() is None
        assert schedule.idle_seconds() is None


def test_cancel_job(set_up, mock_job):
    def stop_job():
        return schedule.CancelJob

    every().second.do(stop_job)
    mj = every().second.do(mock_job)
    assert len(schedule.jobs) == 2

    schedule.run_all()
    assert len(schedule.jobs) == 1
    assert schedule.jobs[0] == mj

    schedule.cancel_job("Not a job")
    assert len(schedule.jobs) == 1
    schedule.default_scheduler.cancel_job("Not a job")
    assert len(schedule.jobs) == 1

    schedule.cancel_job(mj)
    assert len(schedule.jobs) == 0


def test_cancel_jobs(set_up):
    def stop_job():
        return schedule.CancelJob

    every().second.do(stop_job)
    every().second.do(stop_job)
    every().second.do(stop_job)
    assert len(schedule.jobs) == 3

    schedule.run_all()
    assert len(schedule.jobs) == 0


def test_tag_type_enforcement(set_up, mock_job1):
    job1 = every().second.do(mock_job1)
    pytest.raises(TypeError, job1.tag, {})
    pytest.raises(TypeError, job1.tag, 1, "a", [])
    job1.tag(0, "a", True)
    assert len(job1.tags) == 3


def test_get_by_tag(set_up, mock_job):
    every().second.do(mock_job).tag("job1", "tag1")
    every().second.do(mock_job).tag("job2", "tag2", "tag4")
    every().second.do(mock_job).tag("job3", "tag3", "tag4")
    # Test None input yields all 3
    jobs = schedule.get_jobs()
    assert len(jobs) == 3
    assert {"job1", "job2", "job3"}.issubset(
        {*jobs[0].tags, *jobs[1].tags, *jobs[2].tags}
    )
    # Test each 1:1 tag:job
    jobs = schedule.get_jobs("tag1")
    assert len(jobs) == 1
    assert "job1" in jobs[0].tags
    # Test multiple jobs found.
    jobs = schedule.get_jobs("tag4")
    assert len(jobs) == 2
    assert "job1" not in {*jobs[0].tags, *jobs[1].tags}
    # Test no tag.
    jobs = schedule.get_jobs("tag5")
    assert len(jobs) == 0
    schedule.clear()
    assert len(schedule.jobs) == 0


def test_clear_by_tag(set_up, mock_job1, mock_job2, mock_job3):
    every().second.do(mock_job1).tag("tag1")
    every().second.do(mock_job2).tag("tag1", "tag2")
    every().second.do(mock_job3).tag("tag3", "tag3", "tag3", "tag2")
    assert len(schedule.jobs) == 3
    schedule.run_all()
    assert len(schedule.jobs) == 3
    schedule.clear("tag3")
    assert len(schedule.jobs) == 2
    schedule.clear("tag1")
    assert len(schedule.jobs) == 0
    every().second.do(mock_job1)
    every().second.do(mock_job2)
    every().second.do(mock_job3)
    schedule.clear()
    assert len(schedule.jobs) == 0


def test_misconfigured_job_wont_break_scheduler(set_up):
    """
    Ensure an interrupted job definition chain won't break
    the scheduler instance permanently.
    """
    scheduler = schedule.Scheduler()
    scheduler.every()
    scheduler.every(10).seconds
    scheduler.run_pending()
