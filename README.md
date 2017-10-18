# Processor Job Queue 

A Redis job queue service processor, which communicates via single-line json messages on stdin, stdout and stderr.

processor-job-queue receives jobs from job_creators, stores them in a FIFO like Redis list and dispatches these jobs to idle workers. It keeps track of idle and busy workers in Redis sets and keeps track of jobs, dispatched (yet to be accepted) and processing, as well as their current progress.

The job queue can handle multiple queues with different groups of workers and supports prioritization (0=highest - 5=default - 9=lowest). New queues are created on the fly, when either a job or a worker carries an inexistent "queue_name" field.

## Basic Functional Pattern

(Prioritization is implemented on the message- and method-level)

#### Constants
* TICK_INTERVAL = 3000
* WORKER_EXPIRY_PERIOD = TICK_INTERVAL * 3
* MAX_LEASE_TIME = TICK_INTERVAL * 3

#### JOBS_QUEUED = "jobs_queued:"
JOBS_QUEUED - Redis List - holds available jobs

#### JOBS_DISPATCHED = "jobs_dispatched:"
JOBS_DISPATCHED - Redis Sorted Set - holds jobs dispatched but not yet accepted


#### JOBS_PROCESSING = "jobs_processing:"
JOBS_PROCESSING - Redis Sorted Set - holds dispatched jobs, which have been 
accepted by a worker and are therefore processing now. Score is the timestamp 
of the job_accepted message at first and the timestamp of the last 
worker_progress message later.


#### JOBS_PROGRESS = "jobs_progress:"
JOBS_PROGRESS - Redis Sorted Set - holds the same jobs as JOBS_PROCESSING but SCORE is the
percentage finished represented by a float between 1 and 0.

#### WORKERS_LAST_SEEN = "workers_last_seen:"
WORKERS_LAST_SEEN - Redis Sorted Set - is there just for maintenance, it holds
the timestamp of the last worker_idle, worker_progress, worker_finished as its
score. On tick we purge workers older than WORKER_EXPIRY_PERIOD from both 
WORKERS_IDLE_SET and WORKERS_BUSY_SET. 

**WARNING:**: 
Workers MUST send and idle or progress message at least on every tick!!!

#### WORKERS_IDLE_SET = "workers_idle:"
WORKERS_IDLE_SET - Redis Set - holds currently idle workers. When a 
worker_idle is received we add the worker to this group and remove it from 
WORKERS_BUSY_SET. When a job is dispatched, we move the worker from 
WORKERS_IDLE_SET to WORKERS_BUSY_SET.

#### WORKERS_BUSY_SET = "workers_busy:"
WORKERS_BUSY_SET - Redis Set - holds currently busy workers. When a 
job_accepted or a worker_progress is received, we add worker to this group and
remove it from WORKERS_IDLE_SET. When a job_finished is received, we move 
the worker back to the WORKERS_IDLE_SET.

**Note:**
LAST_SEEN, IDLE and BUSY are sufficient, as jobs, which have been 
wrongfully dispatched, for whatever reason, will be readmitted to JOBS_QUEUED 
anyways after MAX_LEASE_TIME. So we are fail-safe by design and therefore 
immune to little hick-ups on this end, however likely or unlikeley they are. 





add_job
multi = client.multi();
multi.hset(payload_obj.uuid, "json", JSON.stringify(payload_obj));
multi.lpush([JOB_QUEUE_PREFIX + payload_obj.queue_name, payload_obj.uuid]);
multi.exec();

assign_job
rpop
