# module: schedule
# file: scheduler.py
import datetime
import logging
import time
from collections.abc import Hashable
from typing import List, Optional
from abc import ABC

from schedule.job import CancelJob, Job

logger = logging.getLogger("schedule")


class BaseScheduler(ABC):
    """
    The base class that contains the shared functionality: to create jobs,
    keep record of scheduled jobs and handle their execution.
    """

    def __init__(self) -> None:
        self.jobs: List[Job] = []

    def get_jobs(self, tag: Optional[Hashable] = None) -> List["Job"]:
        """
        Gets scheduled jobs marked with the given tag, or all jobs
        if tag is omitted.

        :param tag: An identifier used to identify a subset of
                    jobs to retrieve
        """
        if tag is None:
            return self.jobs[:]
        else:
            return [job for job in self.jobs if tag in job.tags]

    def clear(self, tag: Optional[Hashable] = None) -> None:
        """
        Deletes scheduled jobs marked with the given tag, or all jobs
        if tag is omitted.

        :param tag: An identifier used to identify a subset of
                    jobs to delete
        """
        if tag is None:
            logger.debug("Deleting *all* jobs")
            del self.jobs[:]
        else:
            logger.debug('Deleting all jobs tagged "%s"', tag)
            self.jobs[:] = (job for job in self.jobs if tag not in job.tags)

    def cancel_job(self, job: "Job") -> None:
        """
        Delete a scheduled job.
        :param job: The job to be unscheduled
        """
        try:
            logger.debug('Cancelling job "%s"', str(job))
            self.jobs.remove(job)
        except ValueError:
            logger.debug('Cancelling not-scheduled job "%s"', str(job))

    def every(self, interval: int = 1) -> "Job":
        """
        Schedule a new periodic job.
        :param interval: A quantity of a certain time unit
        :return: An unconfigured :class:`Job <Job>`
        """
        job = Job(interval, self)
        return job

    def _check_returned_value(self, job, ret):
        if isinstance(ret, CancelJob) or ret is CancelJob:
            self.cancel_job(job)

    @property
    def next_run(self) -> Optional[datetime.datetime]:
        """
        Datetime when the next job should run.

        :return: A :class:`~datetime.datetime` object
                 or None if no jobs scheduled
        """
        if not self.jobs:
            return None
        return min(self.jobs).next_run

    @property
    def idle_seconds(self) -> Optional[float]:
        """
        :return: Number of seconds until
                 :meth:`next_run <Scheduler.next_run>`
                 or None if no jobs are scheduled
        """
        if not self.next_run:
            return None
        return (self.next_run - datetime.datetime.now()).total_seconds()


class Scheduler(BaseScheduler):
    """
    Objects instantiated by the :class:`Scheduler <Scheduler>` are
    factories to create jobs, keep record of scheduled jobs and
    handle their execution.
    """

    def run_pending(self) -> None:
        """
        Run all jobs that are scheduled to run.
        Please note that it is *intended behavior that run_pending()
        does not run missed jobs*. For example, if you've registered a job
        that should run every minute and you only call run_pending()
        in one hour increments then your job won't be run 60 times in
        between but only once.
        """
        runnable_jobs = (job for job in self.jobs if job.should_run)
        for job in sorted(runnable_jobs):
            self._run_job(job)

    def run_all(self, delay_seconds: int = 0) -> None:
        """
        Run all jobs regardless if they are scheduled to run or not.
        A delay of `delay` seconds is added between each job. This helps
        distribute system load generated by the jobs more evenly
        over time.
        :param delay_seconds: A delay added between every executed job
        """
        logger.debug(
            "Running *all* %i jobs with %is delay in between",
            len(self.jobs),
            delay_seconds,
        )
        for job in self.jobs[:]:
            self._run_job(job)
            time.sleep(delay_seconds)

    def _run_job(self, job: "Job") -> None:
        ret = job.run()
        self._check_returned_value(job, ret)
