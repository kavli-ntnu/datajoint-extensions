# -*- coding: utf-8 -*-
"""
Created on Thu Oct  7 15:13:20 2021

@author: simoba
"""
import rq
import time
import random
import threading
import datetime
from tqdm import tqdm

from .monkey_patch import dj
from .jobs import populate, on_populate_fail
from . import populate_manager_api as api
from . import utils


logger = utils.get_logger(__name__)


class PopulateManager:
    def __init__(self, config_path=None):
        """
        Parameters
        ----------
        config_path : str or path-like object, optional
            Path to the config yml file that specifies what tables will be
            processed, and the parameters for each.
            Default: dj_utils/scheduler/config.yml
        """
        self.config_file = config_path
        self.config = None
        self._config_mod_time = None
        self.load_config()
        self.connection = utils.get_connection()
        self.queues = utils.get_all_queues(conn=self.connection)

        self.jobs_finished = {
            k: rq.registry.FinishedJobRegistry(queue=v) for k, v in self.queues.items()
        }
        self.jobs_failed = {
            k: rq.registry.FailedJobRegistry(queue=v) for k, v in self.queues.items()
        }
        self.jobs_in_progress = {
            k: rq.registry.StartedJobRegistry(queue=v) for k, v in self.queues.items()
        }

        logger.info("Submitting jobs to queues: {}".format(self.queues.keys()))
        logger.info("Currently known workers: {}".format(self.get_workers()))
        logger.info(
            "Targeting cycle time of {}".format(
                utils.seconds_to_pretty_time(self.cycle_time)
            )
        )
        self.init_broadcast_progress()

        if self.run_api:
            # Execution args for spawning this api in a separate thread
            kwargs = {
                "host": self.config["manager"]["api.host"],
                "port": self.config["manager"]["api.port"],
                "threaded": True,
                "use_reloader": False,
            }
            self.app_thread = threading.Thread(
                target=api.app.run, daemon=True, kwargs=kwargs
            )
            self.app_thread.start()
            self.init_broadcast_progress()
            logger.info(
                "Broadcasting API on {}:{}".format(kwargs["host"], kwargs["port"])
            )
        return

    def load_config(self, reload: bool = False):
        """
        Loading the configuration file that specifies what to do and how
        Checks to see whether the modified time of the file has changed since it was last loaded. If it has changed,
        then reload the file. Optionally enforce reload

        Currently not able to start or stop the API - that's set at PM initialisation
        """
        current_modified_time = utils.get_populate_config_modified_time(
            self.config_file
        )
        load = False
        if self._config_mod_time is None:
            # Loading the config on first startup
            logger.info("Configuration loaded")
            load = True
        elif self._config_mod_time != current_modified_time:
            # Reloading config because it has changed
            logger.warning(
                "Config has been updated! ({})".format(current_modified_time)
            )
            load = True
        elif reload:
            logger.info("Forced config reload")
            load = True
        else:
            # no change, do nothing
            pass
        if load:
            self.config = utils.get_populate_config(self.config_file)
            self._config_mod_time = current_modified_time
        return

    @property
    def cycle_time(self):
        return self.config["manager"]["target_cycle_time"]

    @property
    def tables(self):
        return self.config["tables"]

    @property
    def run_api(self):
        return self.config["manager"]["api.run"]

    @property
    def api_data(self):
        return api.data

    def get_workers(self):
        """
        Return a reference to each of the registered workers

        If a worker fails to de-register itself on death, then it will appear to
        still be present until it fails a heartbeat check (default 7 minutes)
        """
        return utils.get_all_workers(conn=self.connection)

    def get_worker_status(self):
        """
        Return an overview of the status of each registered worker
        """
        return utils.get_all_worker_states(conn=self.connection)

    def get_queue_status(self):
        """
        Display the status of all queues that the PM knows about (which is set by DJ config, it is not necessarily true
        that the PM attempts to insert jobs to all known queues)
        """
        return utils.get_all_queue_states(conn=self.connection)

    def check_progress(self, table):
        """
        Check progress on the table and broadcast relevant data if necessary
        Using the built in methods: `_jobs_to_do` supports adding arbitrary restrictions, and `target` is necessary for
        TaskSpikesTrackingProxy
        """
        table = table()  # table supplied as an object, not instance.
        logger.info(f"Checking {table.table_name_fqn}")
        cp0 = time.perf_counter()
        key_source = table._jobs_to_do([])  # Empty list of restrictions
        keys_to_do = (key_source - table.target).fetch("KEY")
        num_to_do = len(keys_to_do)
        num_ks = len(key_source)
        num_keys_done = num_ks - num_to_do
        cp1 = time.perf_counter()
        self.broadcast_progress(
            table_name=table.table_name_fqn,
            to_do=num_to_do,
            done=num_keys_done,
            total=num_ks,
            check_time=round(cp1 - cp0, 3),
        )
        return keys_to_do

    def evaluate_table(self, table, params):
        """
        Primary function: given a table_name, process it and distribute jobs to workers

        1. Identify all the keys not yet completed
        2. Check which, if any keys, are available to be queued (by noting them
           as queued in the jobs table)
        3. Break that list up into appropriately sized chunks (a single chunk being
           sent to a single worker, to be processed as a single batch)
        4. Add the appropriate sets of keys to the queue

        Parameters
        ----------
        table : datajoint query object
            Must be a base (defined) class with a `key_source` attribute.
            Derived queries, e.g. `table & restriction` not valid
        params : dict
            Must include the keywords
            schema : str
                name of the schema as imported
            table : str
                name of the table as defined (in code)
            serial : int
                Maximum number of key to be processed as a batch by a single worker
            parallel : int
                Maximum number of worker batches that can be queued simultaneously

        Returns
        -------
        list
            The set of jobs that have been added to the queue
        """
        self.clean_job_table(table, ttl=params["ttl"])
        jobs = table.connection.schemas[table.database].jobs

        batch_size = params["serial"]
        max_batches = params["parallel"]

        keys_to_do = self.check_progress(table)
        random.shuffle(keys_to_do)
        continuing_keys = []
        for i, key in enumerate(keys_to_do):
            if self.config["manager"]["will_queue"]:
                # The first attempted fix (pull #130) to avoid excess Deadlocks didn't work.
                # Functionally, this is still an outgrowth of using a database as a queueing system - it's choking on
                # the traffic involved in moving keys from available, to queued, to reserved, to completed.
                # The next prong of the approach is to reduce one point of contention
                # Include a feature flag to just not record keys as `queued`
                # This will reduce efficiency a bit, but probably be a smaller proportion than the current rate of
                # Deadlock Errors does.
                if jobs.queue(table.table_name, key):
                    continuing_keys.append(key)
            else:
                continuing_keys.append(key)
            if len(continuing_keys) == batch_size*max_batches:
                # Keep listing keys as queued until EITHER we run out of keys to list, OR we reach the maximum number
                # to be processed per cycle.
                # Has to be done as a conditional here to avoid keys that can't be listed (e.g. due to an error or
                # a reservation) from "taking up" space in the batch (particularly problematic at small batch sizes)
                break

        # Split up into appropriate size batches
        # yields a list of lists, e.g.
        # [ [key0, key1, key2], [key3, key4, key5] ]
        # Keys 0-2 will be processed as one batch, keys3-5 as a separate batch
        batched_keys = [
            continuing_keys[i: i + batch_size]
            for i in range(0, len(continuing_keys), batch_size)
        ]
        # This approach to batching will fill each batch up sequentially, and therefore may appear to be inefficient
        # due to assigning fewer parallel batches than are allowed. That is a deliberate choice due to the overhead of
        # forking worker processes. This is an assumption that may be revisited later.

        # Group key batches with other meta data (schema, table, and any other
        # kwargs for populate).
        # Submit key batches to the appropriate queue to be processed.
        batched_jobs = [
            {"table_name": table.table_name_fqn, "keys": key_batch}
            for key_batch in batched_keys
        ]
        keys_assigned = sum([len(bj["keys"]) for bj in batched_jobs])
        keys_not_assigned = len(keys_to_do) - len(continuing_keys)
        if batched_jobs:
            logger.info(
                "{tn} has divided {num_keys} keys among {num_jobs}"
                " jobs on queue {queue}; {num_ignored} keys ignored".format(
                    tn=table.table_name_fqn,
                    num_keys=keys_assigned,
                    num_jobs=len(batched_jobs),
                    queue=params["queue"],
                    num_ignored=keys_not_assigned,
                )
            )
        queued_jobs = []
        appropriate_queue = self.queues[params["queue"]]
        for batched_job in batched_jobs:
            job_id = appropriate_queue.enqueue(
                f=populate,
                kwargs=batched_job,
                job_timeout=params["job_timeout"],
                ttl=params["ttl"],
                on_failure=on_populate_fail,
            )
            queued_jobs.append(job_id)
        return queued_jobs

    def run_once(self, verbose=False):
        """
        Run `evaluate_table` over all known tables to be evaluated

        Also return the set of queued jobs
        """
        logger.info("Cycle start")
        ro0 = time.perf_counter()
        self.load_config()
        queued_jobs = []
        for table, params in (
            tqdm(self.tables.items()) if verbose else self.tables.items()
        ):
            queued_jobs += self.evaluate_table(table, params)
        logger.info(f"Assigned {len(queued_jobs)} jobs to workers")
        ro1 = time.perf_counter()
        duration = ro1 - ro0
        self.broadcast_status(previous_cycle_time=duration)
        return queued_jobs, duration

    def run_until(self, end, verbose=False):
        """
        Continuously poll the specified database tables for updates for a specified
        amount of time

        The polling frequency is set at a lower limit by `cycle_time`, and at
        an upper limit by how long it takes to iterate through all tables.
        Defaults to once-per-minute.

        If, for some reason, the PM cannot keep up at this rate, then the best
        solution would be to spawn multiple PMs, each evaluating a subset of
        the required tables.  (i.e. pass each PM a different config file)

        Parameters
        ----------
        end : int, float, datetime, NoneType
            Determine how long to run for.
            An int or float will be considered a length of time in SECONDS
            A datetime will be considered to be a specific end time. A datetime
            in the past will trigger an error
            if end is None, then the function will run forever
        verbose : bool, optional
            Display a progress bar or not
            Default False
        """
        logger.info("Populate Manager starting")
        if isinstance(end, datetime.datetime):
            if end < datetime.datetime.now():
                # i.e. the end time is in the past, relative to the host's clock
                raise Exception("You cannot specify an end time in the past")
        elif isinstance(end, (float, int)):
            end = datetime.datetime.now() + datetime.timedelta(seconds=end)
        elif end is None:
            # Logic for this case handled later, just included in if/else for completeness
            pass
        else:
            raise Exception(
                f"`end` should be one of types (int, float, datetime, None), not'{type(end)}"
            )
        logger.info("Process manager beginning")
        if end is not None:
            logger.info("End datetime: {}".format(utils.time_format(end)))
        while True:
            if end is not None and datetime.datetime.now() > end:
                break
            _, duration = self.run_once(verbose)
            self.delay_next_cycle(duration)
        logger.info("Populate Manager stopping")
        return

    def run_forever(self, verbose=False):
        """Run without a time limit"""
        self.run_until(end=None, verbose=verbose)

    def run_blank(self):
        """A testing function for the api - don't queue any jobs, just check on progress"""
        logger.info("Populate Manager starting dry run")
        while True:
            rb0 = time.perf_counter()
            self.load_config()
            for table in self.tables.keys():
                self.check_progress(table)
            rb1 = time.perf_counter()
            duration = rb1 - rb0
            self.broadcast_status(previous_cycle_time=duration)
            self.delay_next_cycle(duration)

    def delay_next_cycle(self, previous_cycle_duration):
        """
        Delay the start of the next cycle (if needed) to maintain the desired minimum cycle time
        If waiting for a long time, periodically refresh the data displayed in the API
        """
        to_wait = max(self.cycle_time - previous_cycle_duration, 0.0)
        logger.info(
            "Cycle finished in {}".format(
                utils.seconds_to_pretty_time(previous_cycle_duration)
            )
        )
        logger.info(f"Waiting for {to_wait:.3f} seconds")

        next_cycle_start_time = datetime.datetime.now() + datetime.timedelta(
            seconds=to_wait
        )
        broadcast_delay = 10
        while to_wait > broadcast_delay:
            time.sleep(broadcast_delay)
            self.broadcast_status(previous_cycle_duration)
            to_wait = (next_cycle_start_time - datetime.datetime.now()).total_seconds()
        else:
            time.sleep(to_wait)
        return

    def clean_job_table(self, table, ttl: float = None):
        """
        Based on the ttl of jobs, remove jobs from the JobsTable that are still
        listed as `queued`. Doesn't affect the redis queue (that should be taken
        care of by Redis itself, based on the ttl)
        """
        if ttl is None:
            ttl = self.tables[table]["ttl"]
        jobs = table.connection.schemas[table.database].jobs
        queued_ages = jobs.proj(
            age="TIMESTAMPDIFF(SECOND, `timestamp`, CURRENT_TIME())"
        )
        (jobs & (queued_ages & f"age >= {ttl}" & {"status": 'queued', "table_name": table.table_name})).delete()
        logger.debug(f"Pruned stale queued jobs for `{table.table_name_fqn}`")
        return

    def mass_clean_job_table(self, ttl: float = None):
        """
        Prune all queued jobs at once
        """
        for table in self.tables.keys():
            self.clean_job_table(table, ttl)
        return

    def clean_registry(self):
        """
        Removes expired references to jobs for each queue. That includes references to failed and finished jobs,
        and to jobs that have been queued for longer than their `ttl`.
        """
        for q_name, queue in self.queues.items():
            if queue.acquire_cleaning_lock():
                logger.info("Cleaning registries for '{}'".format(q_name))
                rq.registry.clean_registries(queue)
                rq.worker_registration.clean_worker_registry(queue)
        return

    def broadcast_progress(
        self, table_name: str, to_do: int, done: int, total: int, check_time: float
    ):
        """
        Trigger after checking each table to update the exported api for a watchdog

        This is almost certainly not best practice, but just manipulate the static `data` dictionary in api, which
        the flask server will present

        TODO: What happens if the config changes and removes tables? Should have something somewhere to handle
        TODO: that case and delete them from the shared dict. Might be here, might be in the config load function
        """
        self.api_data["tables"][table_name] = {
            "keys_to_do": to_do,
            "keys_done": done,
            "all_keys": total,
            "progress": None if not total else round(done / total, 3),
            "checked_duration_seconds": check_time,
            "checked_duration_pretty": utils.seconds_to_pretty_time(check_time),
            "checked_time": utils.time_format(),
        }
        self.api_data["manager"]["queues"] = self.get_queue_status()
        self.api_data["manager"]["workers"] = self.get_worker_status()
        self.api_data["manager"]["last_update_time"] = utils.time_format()
        return

    def broadcast_status(self, previous_cycle_time: float = None):
        """
        Trigger once each round (i.e. once per check-all-tables
        """
        self.api_data["manager"] = {
            "database.prefix": dj.config["custom"]["database.prefix"],
            "target_cycle_time": self.cycle_time,
            "previous_cycle_time_seconds": previous_cycle_time,
            "previous_cycle_time_pretty": utils.seconds_to_pretty_time(
                previous_cycle_time
            ),
            "last_update_time": utils.time_format(),
            "num_tables": len(self.tables),
            "workers": self.get_worker_status(),
            "queues": self.get_queue_status(),
        }
        return

    def init_broadcast_progress(self):
        self.broadcast_status()
        for table in self.tables.keys():
            self.broadcast_progress(table.table_name_fqn, None, None, None, None)
        return
