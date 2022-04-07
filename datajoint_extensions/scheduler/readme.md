# Job scheduling

## `scheduler`

The `scheduler` module provides an attempted solution to this problem (see below)

Workers now come in two classes:
* PopulateManager
* rq worker

The PopulateManager is responsible for interrogating a set of tables, defined in a configuration file, and determining what keys should be sent for calculation and with what priority. Keys so chosen are marked as `queued` in the database jobs table, and sent to a Redis queue.

The workers are reused without modification from the Python package [rq](https://python-rq.org/docs/workers/) ([gh](https://github.com/rq/rq)). Each worker listens to one or more Redis queues (with a specified priority order of listening). Once a job arrives via that queue, the worker:
* Forks a child process (called a "work horse" in rq) to actually run the computation
* The child process looks up what table it should be working on from the job arguments. It attempts to import that table - and must be able to do so! - and then runs make() for each of the keys it was sent
* The job has a specified timeout - if that expires while the job is still in progress, the job is killed, and any keys from that job still marked as "queued" are removed from the jobs table.

Due to the way the workers function - fork then import - they handle pymysql's non-thread-safety quite effectively, but incur a certain amount of overhead thereby. I estimate that it adds approximately 5s of dead time per job.

Therefore, most jobs handed to workers are batches of multiple keys, sometimes _many_ keys (e.g. 1000 TaskSpikesTracking at once).

### PopulateManager configuration

A YAML file of the format:

```yaml
manager:
  target_cycle_time: int_seconds
  api.run: bool
  api.port: int
  api.host: str

tables:
  ephys.acquisition.Recording:
    ttl: str
    job_timeout: str
    serial: int
    parallel: int
    queue: str
```

The `tables` key is the primary section.

### Table definitions

IN order to make the solution more generic, the scheduler doesn't _directly_ import the Python code it will work with -
that would require updating the scheduler code any time anything changed. 

Instead, the tables to be processed are given as fully qualified strings - `ephys.acquisition.Recording`, 
ephys.ephys.LFP`. based on this string, both the Manager and the Worker attempt to import the package (and scream loudly
if they cannot).

Each table's parameters are then given. `ttl` and `job_timeout` are given as pretty time strings - e.g. "1h", "2m",
"24h". `serial` and `parallel` respectively determine the size of each batch of keys, and how many batches may be added
to the queue per Manager cycle (i.e. iterating across all listed tables). `queue` specifies which Redis queue the jobs
will be submitted by.

Redis queues do not need to be pre-declared - but are only meaningful if a worker is listening to jobs recieved via that
queue. 


### API

The PopulateManager has a built-in API to export in-progress information. This is run as a Flask microserver in a 
hread, communicating via a shared dictionary. It's about as crude as it gets, and requires future work. 

The api provides a view into:
* Status (`/status`)
  * Table information
    * When was each table last interrogated?
    * How long did that interrogation take?
    * How many keys done and still to do?
  * Queues:
    * How many jobs in each queue?
    * How many workers listening to each queue?
  * Workers:
    * How many workers are active
    * Per worker, their status
      * What queues is it listening to?
      * Is it currently idle or busy?
      * If busy, what table is it working on and how long has it been working on it?
* Manager uptime and health (`/uptime`)

The intention is to use this to provide a much more comprehensive view via the webgui of what is going on and why

### Prioritisation and segmentation

Multiple queues satisfies several requirements at once:
* Some tables are more important than others. The less important tables should be ignored, or at least have reduced 
  dedicated resources while more important jobs exist
* Some tables require massive resources (e.g. LFP needs lots of RAM). The simplest way to manage limited shared
  resources is to set the number of simultaneous workers per host such that in the worst case, resource limits are not exceeded.
* To make efficient use of shared resources, we should ideally be able to switch workers between queues as needed -
  having dedicated low priority workers is an annoyance if they then prevent us running enough workers for higher priority
  things, or requires manual intervention to shift the balance one way or the other

RQ workers solve this problem - to an extent - neatly, by allowing copies of the same fungible, generic, worker to be
assigned different priorities based on which queues each copy listens to and in what order.

The majority of workers are assigned solely to the default queue, and work only on jobs there. A small number of workers
are assigned first to the high-resource queue, and then to default. 

This ensures that low priority jobs will still occur, but not too many at once; and when there are none available, those
workers can switch over to help out the main jobs. 

_In principle_, this will also allow more efficient distribution of resources between 
Ephys and Imaging, by allowing the same set of workers to compute tables from either 
(whatever happens to arrive in the queue). _However_, doing so means updating both pipelines to harmonise their
dependencies to allow installation in the same environment (or to allow some of the more annoying packages to be segmented
off in their own special little environment, with their own special workers. 

Workers can't run cross-environment - they're python code running _within_ an environment. 

### Playing nicely with other workers

Queued keys are co-ordinated both via the `jobs` table and the redis queue in order to interact safely with the native
populate implementation. 

By default, each instance calling `table.populate(reserve_jobs=True, suppress_errors=True)` behaves as follows:
* Look up what keys are available by checking `table.key-source - table`
* For each key, attempt to "claim" it by inserting it as `reserved` in the `jobs` table. If this attempt fails (because
  the key is already errored out, or already reserved), it moves on to the next key. If it succeeds, it calls `table.make(key)`
* If it encounters an exception during `table.make(key)`, it deletes the reservation from the jobs table and replaces it
  with an error
* If it succeeds, it deletes the reservation

The modified populate routine instead works like this:
* `populate(table_name="ephys.ephys.LFP", keys=[...], reserve_jobs=True, suppress_errors=True)`
* Based on the argument `table_name`, it attempts to import the relevant class
* For each keys in `keys`, it will update its status from `queued` to `reserved`. This uses a modified version of the
  reservation code that is aware of the existence and meaning of `queued`. If the job is already reserved (which shouldn't
  happen, but could), it will skip the key
* After reservation, it will proceed as normal: run `table.make(key)`, and either complete (by deleting the reservation)
  or error out (replace the reservation with an error) as necessary.
* The job has an additional on_failure callback function. This doesn't react to individual _keys_ erroring out - that is
  handled by logging in the jobs table - but if something about the job fails, e.g. by running out of time, an unexpected
  error in `populate`, unable to import the table, etc - it identifies all of the queued or reserved keys _associated with
  that batch_ and deletes them from the jobs table

By including the queued and reserved statuses in the jobs table, native and new implementations can work together. HOWEVER,
since the popeulate manager will "reserve" many keys at once (`serial * parallel`, per table, per cycle), the native
implementation may find relatively little to _do_. In principle, the native implementation could be updated to "steal"
queued keys to do without harm, but that would require some updates to include the monkey patching for the whole pipeline,
rather than just the scheduler. 



### Sharing resources across pipelines

`rq` is a Python package, and therefore exists _within_ a given environment. I have absolutely no idea how difficult it
would be to modify the fork-spawn pattern to have the workhorse spawn in a different environment.

For now, the solution to running both pipelines with PopulateManager is to entirely segregate them from each other. They
can share a single Redis instance, but they will need separate PopulateManagers, separate Queues, separate Workers.

This is not perfectly ideal - i.e. it requires bisecting the available compute resources in advance to provide capacity
for each, with limited ability to swap capacity from one to the other. 

_In theory_, the end goal is to find a way such that a single worker instance is able to process jobs from _either_
pipeline. This would be the most efficient solution, and avoid the need to arbitrarily segment available resources (part
A only works on Imaging, part B only works on Ephys). However, this is not currently possible due to the complexity of
the various dependencies.

It _may_, in future, become possible to harmonise _most_ dependencies, e.g. by throwing the very worst dependencies out
into their own venvs (e.g. (ephys, imaging), (suite2p), (dlc-ephys, dlc-imaging)). This is currently only a pipedream.



## The problem

Currently, workers are implemented as a Python script that looks like so:

```python
while continue:
    table1.populate(max_calls=X)
    table2.populate(max_calls=Y)
    table3.populate(max_calls=Z)
    sleep(60)
```

Scaling up total worker capacity means spawning additional copies of this script. 

Each call of `.populate` forces the database server to figure out the set of keys that _should_ be present, compared to 
the set of keys that _are_ present. Thus, the database workload scales linearly with the number of workers, and _also_,
_increases_ when there is minimal work to do - because the workers spend less time doing work, and so ask for new work
more often. 

The primary issue is that "what jobs should I do?" is, potentially, a _very_ expensive query to run. Thus, we hit a
scaling issue when the constant spam of "what should I do?" consumes all DB resources. Scaling up resources on the DB
server is rather harder to do than finding additional capacity to run populate jobs on.




A somewhat separate, but related, issue is that if the workers run as batch jobs, you run into the potentially awkward
case of overlap between batches that don't end when they should. This is more of an operating system level problem than
a Datajoint problem. This case is partially self-solving - if things are idle (greatest risk of resource depletion) then
the old batch job _should_ end soon anyway. If things are busy, then it may result in some weird behaviour. 



## What should a solution look like?

There are several ways to approach a solution, but a solution should, at minimum, address the following points:

* Mostly idle workers should consume fewer, dbserver resources, not more
* The dbserver resource load should scale sub-linearly with the number of workers. It probably can't be zero scaling,
  but must be below _O(n)_

My _current_ thoughts are as follows:

* It is not possible to completely avoid asking the DB "what jobs need doing?" - that is, fundamentally, how the pipeline
  _works_. So the goal is instead to minimise how much effort the DB must spend answering that question _while still
  maintaining sufficient worker capacity to keep up with demand_
* That question is baked into the `.populate` function in Datajoint. Therefore, _either_:
  * We should write our own replacement to that function, _or_
  * We should have some way of switching workers on/off based on load, more complex than "just sleep for a bit each time
    round"



### PopulateManager pattern

* A single process that runs at all times, iterating across all schemas and tables, to determine how many/what jobs need
  doing. (_PopulateManager_)
* Multiple worker processes running in the background. These processes do nothing until/unless signalled by the
  ProcessManager to begin doing something. Several ways these could run
  * Wake-on-command: the Process Manager tells 4 workers to start on table X. Those 4 workers then run `.populate`, then go back to sleep once it finishes
    * This is the simpler case to run, and solves the case of increased-load-during-idle
    * It significantly improves, but does not fully solve, db resource usage during extended computation. Workers will only ever query the db if jobs are known to need doing, but if the manager detects that Table X needs 4 workers, the "what does table X need doing" question will still be asked 5 times in quick succession. 
  * Compute-on-command: the Process Manager tells 4 workers to run keys {1..250} on table X. Those 4 workers run an alternative command to `.populate` (which is superficially similar, but _doesn't_ query for what keys need to be run, because it has been provided with the keys by the ProcessManager)
    * This is the most efficient on dbserver resources, because the expensive query is only ever run by the PM
    * If multiple PMs are running, it could significantly raise the rate at which compute resources are wasted on duplication (i.e. need some way of orchestration)
    * It requires some way to keep track of which keys have been distributed to workers but not yet processed by those workers, to avoid the potential case of the PM queuing duplicate jobs faster than the workers can compute those jobs. That tracking is a definite point of fragility.
* Interprocess communication - could be done with sockets, but a queue would be more sensible. 
  * `multiprocessing` queues _work_, but require everything to be spawned within the same script (I think)
  * `redis` or `celery` offer the options to completely separate the ProcessManager from the workers, which avoids some of the concurrency headaches in Python. Also supports having a single ProcessManager commanding workers across multiple independent hosts. 





### Big memory jobs

There are three tables which include high-memory requirements on some (most) jobs - LFP, Waveform, SpikeDepths. Plus there is an extremely slow and moderately memory demanding table - ShuffledScores.

Ideally there would be some way to select out the easier jobs from these tables and submit them to the "normal" workers, reserving the biggest jobs to be done in an environment where they won't run into memory errors (which, in practice, means limiting the concurrency, and only running on a high RAM machine).

As a stretch goal, ideally there would be some level of management per-host so that, e.g., a host might be allowed to do 3 1-hour recordings in parallel, but if a 2 or 3hr recording is started, prevent any other big memory jobs starting at all. This is a much more complicated task, though, as then each worker has to be aware of and communicate with its immediate neighbours

Several options:
* Don't bother: treat all potential tasks the same, and limit concurrency regardless of actual size. The easiest solution
* Include some sort of table-specific logic to decide what is a "simple" job, and what is not. Have a separate queue (and separate workers?) to handle the big jobs
  * This means adding some kind of additional logic to the PopulateManager to support per-table restrrictions (and different queues as a result). 
  * That would allow more fine-tuned job binning - e.g. LFPs for up to 1hr go in the basic job queue; LFPs up to 3 hours go in the high-memory queue, and above that go in the gigantiqueue which has exactly one worker processing them (and which will probably keep failing as long as the high-memory queue contains anything
* Have some sort of advanced juggling to try and have each worker host decide how much potential capacity it has, i.e. the workers decide what jobs to accept. 
  * Some kind of mutex / token system per host? A computer would have N mutexes, 1 per 32GB of ram, and large memory jobs would need to claim M of them before accepting a job of a certain size
  * This would require significant re-working to how an `rq worker` deals with jobs - as it stands now, they accept a job in order to find out anything about it, I think. Instead, each worker would need to co-ordinate accepting a job with checking if the resources are actually available and claiming them, then accepting the job, and how to un-accept the job/resources if the other is suddenlly unavailable. It's certainly a stretch goal, but it adds a huge amount of complexity to the project for relatively minimal gain.
