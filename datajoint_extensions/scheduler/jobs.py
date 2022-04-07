"""
The set of jobs that might be run by the (remote) workers
At minimum, the remote worker must be able to successfully import this file

The ProcessManager must be able to import this file and access the method signature
of any method sent to the workers, BUT in the event of a mismatch between the
manager and remote workers, the code that is actually executed is that present
on the remote workers. 
"""

import signal
import random
import traceback
import numpy as np
from tqdm import tqdm

from .utils import get_table, get_logger
from .monkey_patch import dj, key_hash
# Note! the key_hash function was renamed for 0.13.3
# That version split is handled in .monkey_patch, and the handling is reused by importing it from there

logger = get_logger(__name__)


def populate(
    table_name: str,
    keys: list,
    suppress_errors: bool = True,
    return_exception_objects: bool = False,
    reserve_jobs: bool = True,
    order: str = "original",
    max_calls: int = None,
    display_progress: bool = False,
):
    """
    Homegrown alternative to Datajoint's in-built autopopulate mixin

    The primary differences are that this method expects to be _given_ specific
    keys to process, rather than looking them up itself. This approach is more
    complex - it requires some manager process, somewhere else, feeding it the
    appropriate keys to work with. However, it should be substantially more
    performant, due to not requiring every worker instance to separately force
    the dbserver to re-produce the list of keys to be done.

    The logic is extensively lifted from `Autopopulate.populate` within Datajoint
    (see https://github.com/datajoint/datajoint-python/blob/master/datajoint/autopopulate.py)

    Parameters
    ----------
    table_name: str
        Fully qualified name of the table from the outermost package to the table
        such that it can be imported and accessed via `importlib` and `getattr`
        Example: 'ephys.analysis.TaskSpikesTracking.key_source'
    keys : dict or list of dict
        The set of keys that should be processed
    suppress_errors : bool, optional
        If True, exceptions will be logged and then the worker will move on to the next key
        If False, the worker will crash and display the full traceback to the user
        Default True
    return_exception_objects : bool, optional
        If `suppress_errors` is true, then after completion, the set of (any) exception objects
        will be returned to the user
        Default False
    reserve_jobs: bool, optional
        If True, co-ordinate jobs in progress via the schema `Jobs` table by marking specific keys as
        'reserved'. In principle not required for the advanced populate logic, since keys _should_ not
        be queued multiple times. There is a mild performance penalty to leaving as True, but it's worth
        it for ensuring continued co-ordination with the default populate mechanism
        Default True
    order: str, optional
        The order in which keys are processed. Options are `original`, `reverse`, `random`.
        Useful for the default populate logic where a large number of jobs exist, to help avoid
        DuplicateErrors (since the jobs table is not perfect at assuring unique job assignments).
        Mostly irrelevant for the advanced mechanic which should be better at assigning keys exactly
        once
        Default "original"
    max_calls: int, optional
        The maximum number of keys to process in one go. Irrelevant for advanced populate, where the
        number of keys is determined by the management process
        Default None (i.e. no limit)
    display_progress: bool, optional
        Display a tqdm progress bar during populate?
        Default False

    Returns
    -------
    None.
    """
    logger.info("Processing {} keys for table {}".format(len(keys), table_name))
    schema, table = get_table(table_name)
    # A complicating factor is that some tables are instantiated by default
    # e.g. the key source fillers, and some are not (default case)
    # `make()` is an instance method and therefore requires the object to be
    # instantiated. Try/Catch to avoid calling already instantiated objects
    try:
        table = table()
    except TypeError:
        pass

    if isinstance(keys, dict):
        keys = [
            keys,
        ]
    if not isinstance(keys, (list, tuple, np.ndarray)):
        raise Exception(
            "'keys' must be provided as a dictionary or a list of"
            " dictionaries. You have provided `{}`".format(type(keys))
        )

    if table.connection.in_transaction:
        raise dj.DataJointError("Populate cannot be called during a transaction.")

    valid_order = ["original", "reverse", "random"]
    if order not in valid_order:
        raise dj.DataJointError(
            "The order argument must be one of {}".format(str(valid_order))
        )
    error_list = [] if suppress_errors else None
    jobs = schema.schema.jobs if reserve_jobs else None

    # define and setup signal handler for SIGTERM
    if reserve_jobs:

        def handler(signum, frame):
            raise SystemExit("SIGTERM received")

        old_handler = signal.signal(signal.SIGTERM, handler)

    if order == "reverse":
        keys.reverse()
    elif order == "random":
        random.shuffle(keys)

    call_count = 0

    make = table._make_tuples if hasattr(table, "_make_tuples") else table.make

    for key in tqdm(keys, desc=table.__class__.__name__) if display_progress else keys:
        if max_calls is not None and call_count >= max_calls:
            break
        if not reserve_jobs or jobs.reserve(table.table_name, key):
            table.connection.start_transaction()
            if key in table:  # already populated
                table.connection.cancel_transaction()
                if reserve_jobs:
                    jobs.complete(table.table_name, key)
            else:
                call_count += 1
                table.__class__._allow_insert = True
                try:
                    make(dict(key))
                except (KeyboardInterrupt, SystemExit, Exception) as error:
                    try:
                        table.connection.cancel_transaction()
                    except dj.errors.LostConnectionError:
                        pass
                    error_message = "{exception}{msg}".format(
                        exception=error.__class__.__name__,
                        msg=": " + str(error) if str(error) else "",
                    )
                    if reserve_jobs:
                        # show error name and error message (if any)
                        jobs.error(
                            table.table_name,
                            key,
                            error_message=error_message,
                            error_stack=traceback.format_exc(),
                        )
                    if not suppress_errors or isinstance(error, SystemExit):
                        raise
                    else:
                        error_list.append(
                            (key, error if return_exception_objects else error_message)
                        )
                else:
                    table.connection.commit_transaction()
                    if reserve_jobs:
                        jobs.complete(table.table_name, key)
                finally:
                    table.__class__._allow_insert = False
    # place back the original signal handler
    if reserve_jobs:
        signal.signal(signal.SIGTERM, old_handler)
    logger.info(
        "Processed {num_keys} for table {tbl_name}".format(
            num_keys=len(keys), tbl_name=table.table_name
        )
    )
    return error_list


def populate_trigger_exception(*args, **kwargs):
    """
    A test function deliberately designed to fail and trigger the failure callback
    """
    raise Exception("Oh no, a failure")


def on_populate_fail(job, connection, result, *args, **kwargs):
    """
    Some aspect of the batch job failed, which has probably left some of the keys
    marked in the job table as queued, but they aren't actually in the queue anymore
    This function unmarks them, and makes them available to be re-calculated in
    future

    Because of the way that the populate function is structured, this callback
    doesn't need to worry about writing errors into the job table - that's
    already done. Instead, this function only has to mark any rows in the table
    (that are associated with this *batch*) that are still `queued` as no longer
    `queued`.
    """
    table_name = job.kwargs.get("table_name")
    keys = job.kwargs.get("keys")
    schema, table = get_table(table_name)
    key_hashes = [
        {"table_name": table.table_name, "key_hash": key_hash(key)}
        for key in keys
    ]
    jobs = schema.schema.jobs
    jobs_queued_but_not_completed = (
        jobs & key_hashes & "status IN ('queued', 'reserved')"
    )
    logger.info(
        "Discarding {} queued keys for table {}: job failed".format(
            len(jobs_queued_but_not_completed), table_name
        )
    )
    jobs_queued_but_not_completed.delete_quick()
    return
