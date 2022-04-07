import rq
import time
import yaml
import redis
import inspect
import pathlib
import logging
import importlib
from datetime import datetime

from .monkey_patch import dj

# Have to import the patched version of DJ BEFORE importing any other datajoint
# objects (which is done by `get_table`, in this file)

# This setup should allow this file to also be used as a config file for the
# RQ workers, see https://python-rq.org/docs/workers/

config_redis = dj.config.get("redis", {})
REDIS_HOST = config_redis.get("host")
REDIS_PORT = config_redis.get("port")
REDIS_DB = config_redis.get("database")

# Included here to keep the config-touching code in one place, but NOT used as
# part of the `rq worker` setup (that would require a var called `QUEUES`)
# rq worker queue setup is deliberately handled elsewhere to allow prioritisation between workers and queues
KNOWN_QUEUES = config_redis.get("queues")
DEFAULT_CONFIG_FILE = pathlib.Path(__file__).parent / "config.yml"


def get_logger(name: str):
    """
    A common method to provide a pro-forma logger in each of the relevant files
    """
    logger = logging.getLogger(name)
    logger.propagate = False  # avoid importing/changing the logging done in the Datajoint module
    formatter = logging.Formatter(
        # Removed `%(name)s:` because it's long and unhelpful
        fmt="%(asctime)s:%(levelname)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, dj.config.get("loglevel", "WARNING")))
    return logger


logger = get_logger(__name__)


def get_populate_config_path(path=None):
    if path is None:
        path = DEFAULT_CONFIG_FILE
    if not isinstance(path, pathlib.Path):
        path = pathlib.Path(path)
    return path


def get_populate_config_modified_time(path=None):
    """
    Determine when the config was last changed. Used to decide whether to reload
    """
    path = get_populate_config_path(path)
    time_modified = time_format(datetime.fromtimestamp(int(path.stat().st_mtime)))
    return time_modified


def get_populate_config(path=None):
    """
    Load details about what tables to populate from an external configuration file

    This is more flexible than coding the configuration directly into this file,
    BUT it breaks the linting link - the schemas must be available to be imported
    but nothing will actively refer to them for the linter to refer to.

    Configuration format
    --------------------
    YAML text file
    Each table should be given as per the following example

    ephys.acquisition.Recording:
        ttl: 12345
        job_timeout: 34567
        serial: 4
        parallel: 5
        queue: my-queue-name

    Time values can be given either as numbers (int/float in seconds), or as a
    string including the time unit, e.g. "1s", "2m", "3h", "4d". Only a single
    time unit is supported, e.g. "3h4m" is invalid. Time units are parsed via
    rq.util.parse_timeout

    ttl : how long the job will remain in the Redis queue before being killed off
          due to lack of available workers. The ProcessManager will also attempt
          to remove `queued` jobs from the JobsTable no sooner than this time later
          (but not guaranteed to be immediately on ttl expiry)
    job_timeout : how long the worker will be given before the _entire batch_ times
                  out and is marked as "failed".
    serial : how many jobs should be assigned as a bunch for a single worker to
            handle as a single batch
    parallel : how many workers can be started in each cycle of checking the
                entire schema. It does not prevent additional worker-start orders
                being sent the next time the PM checks through the queue
    Jobs are created by filling up each "batch" by the number `serial`, so it is
    not guaranteed that the entire number `parallel` will be started, just that
    no more than this number will be (per Process Manager pass)
    In combination, `serial` and `parallel` determine the maximum throughput of
    the pipeline on that particular table (if not otherwise limited by, e.g.,
    available compute workload)

    The queue name should be a member of `dj.config["redis.queues"]` (list)

    TODO: Support pretty time for cycle time input

    Parameters
    ----------
    path : str or pathlib object
        Path to the configuration file

    Returns
    -------
    dict
        Configuration specifying each table and the parameters of how that table
        should be populated
        The keys are references to the table *class* themselves
    """
    path = get_populate_config_path(path)
    with open(path, "r") as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
    manager_config = config.get("manager", {})
    tables_config = config.get("tables", {})

    # ==== tables_config ====
    # Iterate across all keys, checking that:
    #  1. The schema/table matches an object that is accessible in code
    #  2. the expected k:v pairs are present
    #  3. Convert timeouts to numbers of seconds
    #  4. Move to a new structure where the table _itself_ is the key
    #     Not a string name
    expected_table_keys = (
        "table_name",
        "ttl",
        "job_timeout",
        "serial",
        "parallel",
        "queue",
    )
    time_keys = ("ttl", "job_timeout")

    tables_obj = {}

    for key, value in tables_config.items():
        # 1. given a string name, e.g., "acquisition.Recording", try to resolve
        #    the actual objects
        # Bit of a hack - store the fully qualified name in the table as well, to
        # make the api display a bit cleaner
        _, table_object = get_table(key)
        table_object.table_name_fqn = key
        value["table_name"] = key

        # 2. Check that the expected data is present
        for expected_key in expected_table_keys:
            assert (
                expected_key in value.keys()
            ), f"`{expected_key}` is missing from `{key}`"
        assert (
            value["queue"] in KNOWN_QUEUES
        ), f"{value['queue']} is not a valid queue name ({KNOWN_QUEUES})"

        # 3. Convert time values
        for time_key in time_keys:
            value[time_key] = rq.utils.parse_timeout(tables_config[key][time_key])

        # 4. Change main key
        tables_obj[table_object] = value

    # ==== manager_config ====
    expected_manager_keys = {
        "target_cycle_time": 600,
        "api.run": False,
        "api.port": None,
        "will_queue": True,
    }
    for key, value in expected_manager_keys.items():
        if key not in manager_config.keys():
            manager_config[key] = value
        else:
            pass  # the value is there already

    # And bundle everything back up as one big dictionary
    config["manager"] = manager_config
    config["tables"] = tables_obj

    return config


def get_table(name: str):
    """
    Attempt to import a Python object based on its full qualified name. That is,
    the entire path from the outermost package to the module, class, method,
    property or other Python object. Instance objects (i.e. any method or property
    depending on `self`) cannot be imported

    Approach inspired by, and based on, the implementation in `rq`, but with some
    adaptions to support importing the `key_source` tables where necessary

    Parameters
    ----------
    name: str
        fully qualified name of an importable object
        Examples of fully qualified object names:
            numpy.ma.MaskedArray
            scipy.optimize.curve_fit
            ephys.acquisition.Recording.RecordingSync

    Returns
    -------
    module:
        The closest-level module to the requested object - within DJ, this
        will usually have the `.schema` attribute for access to internals.
    object:
        The requested object itself

    Raises
    ------
    ModuleError
        If no module matching any element of the name can be imported
    AttributeError
        After importing the module, if any stage of navigating the attribute
        tree fails
    """
    t0 = time.time()
    module_bits = name.split(".")
    attribute_bits = []
    module = None
    while len(module_bits):
        try:
            module_name = ".".join(module_bits)
            module = importlib.import_module(module_name)
            break
        except ModuleNotFoundError:
            attribute_bits.insert(0, module_bits.pop())
    if module is None:
        raise ModuleNotFoundError(f"Cannot import '{module_bits[0]}'.")

    if not len(attribute_bits):
        return module, None
    obj = module
    for attribute_name in attribute_bits:
        if hasattr(obj, attribute_name):
            obj = getattr(obj, attribute_name)
        else:
            raise AttributeError(f"'{obj}' does not have attribute '{attribute_name}'")
    t1 = time.time()
    logger.debug(f"Fetched '{obj}' in {(t1 - t0):.3n} seconds")
    return module, obj


def get_connection():
    """
    Makes a new connection
    Log what the function that _called_ this function is called
    """
    logger.info("New connection request from function '{}'".format(inspect.stack()[1].function))
    conn = redis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
    return conn


def get_queue(q_name: str, conn=None):
    """
    Return a reference to an rq Queue object
    """
    if conn is None:
        conn = get_connection()
    queue = rq.Queue(name=q_name, connection=conn)
    return queue


def get_all_queues(conn=None):
    """Get a dictionary of all Queue objects specified via the config file"""
    if conn is None:
        conn = get_connection()
    _queues = {k: get_queue(k, conn) for k in KNOWN_QUEUES}
    return _queues


def get_all_queue_states(conn=None):
    """
    Return a dictionary keyed by queue name, listing number of waiting jobs and number of listening workers
    """
    if conn is None:
        conn = get_connection()
    _queue_status = {
        q_name: {
            "jobs_waiting": len(q),
            "workers": len(get_queue_workers(q)),
        }
        for q_name, q in get_all_queues(conn=conn).items()
    }
    return _queue_status


def get_queue_workers(q_obj: rq.Queue):
    """Get list all of Worker objects currently listening to a queue"""
    _workers = rq.Worker.all(queue=q_obj)
    return _workers


def get_all_workers(conn=None):
    """
    Get dictionary: of all workers split by queue

    Returns
    -------
    dict
        keys are queue_names (str)
        Values are lists of Worker objects that listen to that queue.
        Each worker may appear under multiple queues
    """
    if conn is None:
        conn = get_connection()
    _workers = rq.Worker.all(connection=conn)
    return _workers


def get_worker_state(worker_obj: rq.Worker, conn=None):
    """Given a Worker object, generate a json-compatible representation of its state"""
    if conn is None:
        conn = get_connection()
    worker_dict = {
        "name": worker_obj.name,
        "host": worker_obj.hostname,
        "pid": worker_obj.pid,
        "state": worker_obj.get_state(),
        "queues": [k for k, v in get_all_queues(conn=conn).items() if v in worker_obj.queues],
    }
    time_since_last_heartbeat = (
        datetime.utcnow() - worker_obj.last_heartbeat
    ).total_seconds()
    # NOTE! the rq worker explicitly uses `utc` time, rq.utils.utcnow()
    worker_dict["last_seen_seconds"] = round(time_since_last_heartbeat, 3)
    worker_dict["last_seen_seconds_pretty"] = seconds_to_pretty_time(
        time_since_last_heartbeat
    )
    if worker_dict["state"] == "busy":
        job = worker_obj.get_current_job()
        if job is not None:
            # This seems to happen sometimes and I'm not entirely sure why, but for some reason, a worker appears to
            # be busy, but doesn't have a current job. Maybe it's a race condition of some kind?
            job_time = worker_obj.current_job_working_time
            worker_dict["table"] = job.kwargs.get("table_name")
            worker_dict["current_job_seconds"] = round(job_time, 3)
            worker_dict["current_job_pretty"] = seconds_to_pretty_time(job_time)
            worker_dict["current_job_timeout"] = seconds_to_pretty_time(job.timeout)
    return worker_dict


def get_all_worker_states(conn=None):
    """
    Get the status of all workers in a json friendly format
    """
    if conn is None:
        conn = get_connection()
    _worker_status = {}
    for worker in get_all_workers(conn=conn):
        ws = get_worker_state(worker, conn)
        _worker_status[ws.pop("name")] = ws
    return _worker_status

def worker_shutdown(idle_only=True, conn=None):
    """
    Send a shutdown command to workers

    Per https://python-rq.org/docs/workers/, when a SIGINT or SIGTERM command is received, workers
    will begin a graceful shutdown, i.e. wait for current job to finish, then de-register itself from
    the queue. If a second SIGINT or SIGTERM command is received, any still-running jobs are forcefully
    terminated. The worker will still try to de-register itself from the queue. RQ provides a method
    to send these kill commands via Redis itself, i.e. allowing remote shutdown.

    If `only_idle` is True, then only command idle workers to shut down
    If `only_idle` is false, command all workers to shutdown, delayed by in-progress jobs
    Send the command a second time to forcefully shut down non-idle workers.
    """
    if conn is None:
        conn = get_connection()
    for worker in get_all_workers(conn):
        if (
            not idle_only
            or (idle_only and worker.get_state() == "idle")
        ):
            logger.warning("Shutting down worker '{}'".format(worker.name))
            rq.command.send_shutdown_command(conn, worker.name)
    return


def time_format(t: datetime = None):
    """
    Convert either the provided datetime, or the time right now, to a standard string
    """
    date_fmt = "%Y-%m-%d %H:%M:%S"
    if t is None:
        t = datetime.now()
    return t.strftime(date_fmt)


def seconds_to_pretty_time(t_seconds: float):
    """Convert a time in seconds into a string in weeks, days, hours, minutes, seconds (units with a value zero are
    excluded)

    Timedelta formatter improved from https://stackoverflow.com/a/42320260"""
    constants = {"W": 604800, "D": 86400, "H": 3600, "M": 60, "S": 1}
    if t_seconds is None:
        return None
    elif t_seconds < 0:
        raise ValueError("A duration cannot be negative")
    elif t_seconds < 1:
        return "<1s"
    else:
        pretty_time = {}
        fmt = ""
        for field, const in constants.items():
            num = int(t_seconds / const)
            t_seconds -= num * const
            if num >= 1:
                pretty_time[field] = num
                fmt += "{{{}:02}}{} ".format(field, field.lower())
                # Escaping curly braces inside a string -> double curly braces
        return fmt.format(**pretty_time).strip()  # remove the trailing space
