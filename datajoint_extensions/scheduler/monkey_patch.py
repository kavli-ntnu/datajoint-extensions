"""
Monkey patch the dj JobsTable class to provide a "queued" status
https://github.com/datajoint/datajoint-python/blob/master/datajoint/jobs.py

To make this work, you should include
    from .monkey_patch import dj
before importing anything dj related. As Python explicitly avoids re-importing
modules, the patched version will be used in subsequently imported modules. 
"""
import os
import time
import random
import pymysql
import platform
import datajoint as dj
from datajoint.jobs import JobTable
from datajoint.errors import DuplicateError
try:
    # This hashing function was renamed for Datajoint 0.13.x
    from datajoint.hash import key_hash
    LEGACY = False
except ImportError:
    # For versions earlier than 0.13.3
    from datajoint.hash import hash_key_values as key_hash
    LEGACY = True


def retry_on_deadlock(func):
    """
    Another alternative approach to try and handle the large number of deadlocks that I observe in and around the Jobs
    table

    With `jobs.queue(key)`, `jobs.reserve(key)` and `jobs.complete(key)`, try and catch Deadlockerrors (OperationalError
    1213, Deadlock found when trying to get lock). Back off for a random period of time ([0.5-1.5]s), and then try again

    If the process still fails after N attempts, stop trying.
    """
    def inner(*args, **kwargs):
        count = 0
        max_attempts = 5
        while count < max_attempts:
            count += 1
            try:
                value = func(*args, **kwargs)
            except pymysql.err.OperationalError as e:
                time.sleep(random.random()+0.5)
                if count == max_attempts:
                    raise pymysql.err.OperationalError(e+f" (backoff {count})")
            else:
                break
        return value
    return inner


@retry_on_deadlock
def to_queue(self, table_name, key):
    """
    Mark a job as _queued_ for computation, but not yet reserved (i.e. being
    computed). A job that is so enqueued should not be queued _again_, but may
    be selected for processing.
    """
    job = dict(
        table_name=table_name,
        key_hash=key_hash(key),
        status="queued",
        host=platform.node(),
        pid=os.getpid(),
        connection_id=self.connection.connection_id,
        key=key,
        user=self._user,
    )
    try:
        with dj.config(enable_python_native_blobs=True):
            self.insert1(job, ignore_extra_fields=True)
    except DuplicateError:
        # `status` is a non-pk element, so a job that is marked as either
        # queued or reserved already will be unable to be queued.
        return False
    return True


@retry_on_deadlock
def to_reserve(self, table_name, key):
    """
    This one also needs to be monkey patched to become aware of the `queued`
    status. If a job is already `queued`, then it should be moved to `reserved`
    and return `True`. If the job is already reserved, then the old behaviour
    should apply, return `False`.

    "Old" style commands running `table.populate` will use the old definition of how
    to reserve a job, and will be unable to reserve a job that is already queued. Thus,
    those old processes should be able to run side-by-side. However, depending on how
    many keys are "claimed" by queueing, old processes probably won't be able to accomplish
    very much
    """
    pk = dict(
        table_name=table_name,
        key_hash=key_hash(key),
    )
    job = dict(
        **pk,
        status="reserved",
        host=platform.node(),
        pid=os.getpid(),
        connection_id=self.connection.connection_id,
        key=key,
        user=self._user,
    )
    try:
        """
        The pattern is a bit odd here to avoid deadlock issues with extremely frequent deletions from the jobs table
        Given a (table, key) combination:
        * If the key is already queued, *update* the row to a reservation (including changing host details)
        * If the key is not queued, try to insert the reservation
        This should eradicate Deadlock errors on converting a `queued` entry to a `reserved` entry.
        However, it might just move the problem to Deadlock errors on completion (i.e. deleting a `reserved` entry after
        the job completes)
        """
        if self & pk & {"status": "queued"}:
            if LEGACY:
                # 0.12.x and earlier used `._update(column_name, column_value)
                kws = ("status", "host", "pid", "connection_id", "user")
                for kw in kws:
                    (self & pk)._update(kw, job[kw])
            else:
                # 0.13.x and later use `.update_1(row)`, with the restriction specified _inside_ `row`, i.e. the pk
                # `._update()` will be removed in 0.14.x
                self.update1(job)
        else:
            self.insert1(job, ignore_extra_fields=True)
    except DuplicateError:
        return False
    return True


@retry_on_deadlock
def to_complete(self, table_name, key):
    """
    The only change from the original function is to wrap it with retry
    """
    self.old_complete(table_name, key)

@retry_on_deadlock
def to_error(self, table_name, key, error_message, error_stack=None):
    """
    The only change from the original function is to wrap it with retry
    """
    self.old_error(table_name, key, error_message, error_stack)


# TODO! Need to monkey patch the definition as well to accept a new status type
# This is tricky because the definition is only ever assigned as an instance
# property, never a class one, and it's formatted too (database name)
# In principle, losing the database name is not a problem
# NOTE: monkey patching the definition doesn't need to (and probably shouldn't)
# happen *here* - it needs to happen before initialisation and thus should probably
# inside the ephys pipeline's __init__. It can also *in principle* be done
# via mysql after the fact, although that is somewhat more awkward.

JobTable.queue = to_queue
JobTable.reserve = to_reserve

# Note! We still want access to the old complete and error functions, so rename before replacing
JobTable.old_complete = JobTable.complete
JobTable.old_error = JobTable.error
JobTable.complete = to_complete
JobTable.error = to_error
