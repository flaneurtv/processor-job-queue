var readline = require('readline');
var fs = require('fs');
var redis = require("redis"),
    client = redis.createClient({"host":"redis"});
const TICK_INTERVAL = 3000
const IDLE_EXPIRY_PERIOD = TICK_INTERVAL * 3
const MAX_LEASE_TIME = TICK_INTERVAL * 3
const JOB_QUEUE_PREFIX = "jobs_queued:"
const DISPATCHED_PREFIX = "jobs_dispatched:"
const PROCESSING_PREFIX = "jobs_processing:"
const PROGRESS_PREFIX = "jobs_progress:"
const WORKERS_IDLE_PREFIX = "workers_idle:"

dispatchJob_lua =  'local job_uuid = redis.call("RPOP", "' + JOB_QUEUE_PREFIX + '" .. KEYS[1])\n'
dispatchJob_lua += 'if job_uuid != nil then\n'
dispatchJob_lua += '  redis.call("ZADD", "' + DISPATCHED_PREFIX + '" .. KEYS[1], KEYS[2], job_uuid)\n'
dispatchJob_lua += '  redis.call("HSET", job_uuid, "dispatched_at", KEYS[2])\n'
dispatchJob_lua += '  return redis.call("HGET", job_uuid, "json")\n'
dispatchJob_lua += 'else\n'
dispatchJob_lua += '  return nil\n'
dispatchJob_lua += 'end'

client.on("error", function (err) {
    console.log("Error " + err);
});
client.on("connect", function() {
    console.log("connected");
});

// read the message template
var expandenv = require('expandenv');
var lines = fs.readFileSync('./service-processor/schema-message.json', {"encoding": "utf-8"}).split(/\n/);
var messageTemplate = '';
lines.forEach(function(element) {
    messageTemplate += element.trim();
});

// Reads JSON from stdin
var rl_stdin = readline.createInterface({
    input: process.stdin
});

// When line comes from stdin
rl_stdin.on('line', function(line) {
    dispatchAction(line)
});

function dispatchAction(line) {
    // first we check if the payload is valid JSON
    try {
        message_obj = JSON.parse(line);
    } catch (e) {
        console.error('{"log_level": "error", "created_at": "' + new Date().toISOString() + '", "log_message": "payload is not valid JSON: ' + escapeQuotes(line) + '"}');
        return
    }
        switch(message_obj["topic"].replace(process.env.NAMESPACE_LISTENER + "/job_queue/","")) {
            case "add_job":
                addJob(message_obj.payload)
                break;
            case "request_job":
                dispatchJob(message_obj.payload.queue_name, message_obj.service_uuid)
                break;
            case "complete_job":
                completeJob(message_obj.payload)
                break;
            case "worker_idle":
                workerIdle(message_obj)
                dispatchJob(message_obj.payload.queue_name, message_obj.service_uuid)
                break;
            case "worker_progress":
                workerProgress(message_obj.payload)
                break;
            case "list_job_queue":
                listJobQueue(message_obj.payload)
                break;
            case "list_leased_queue":
                listLeasedQueue(message_obj.payload)
                break;
            case "list_progress_queue":
                listProgressQueue(message_obj.payload)
                break;
            case "list_worker_queue":
                listWorkerQueue(message_obj.payload)
                break;
        }
}

function escapeQuotes(s) {
    return s.replace(/"/g, '\\\"');
}

function addJob(payload_obj) {
    console.error("addJob")
    multi = client.multi();
    multi.hset(payload_obj.uuid, "json", JSON.stringify(payload_obj));
    multi.lpush([JOB_QUEUE_PREFIX + payload_obj.queue_name, payload_obj.uuid]);
    multi.exec();
}

function dispatchJob(queue_name, worker_uuid) {
    console.error("dispatchJob", message_obj)
    dispatched_at = Date.now();
    client.eval(dispatchJob_lua, 2, queue_name, dispatched_at, function(err, res) {

        console.error(err);
        console.error(res);
        job_obj = JSON.parse(res);
        job_obj.dispatched_at = dispatched_at;
        topic = worker_uuid + "/job_assignment"
        stdoutMessage(topic, job_obj);
        /*
           We purge expired workers from the idle list for a particular queue_name,
           whenever a job is leased from this queue. Purging expired workers is not
           a necessity for proper operation. But in case number and UUIDs of
           workers change frequently e.g. due to elastic scaling of workers, it
           makes sense in the long run.
         */
        purgeExpiredWorkers(queue_name, Date.now() - IDLE_EXPIRY_PERIOD)
    });
}

function acceptJob(payload_obj) {
  acceptJob_lua += 'redis.call("ZADD", "' + PROCESSING_PREFIX + '" .. KEYS[1], 0, job_uuid)\n'
  multi = client.multi();
  multi.zrem(DISPATCHED_PREFIX + payload_obj.queue_name, payload_obj.uuid)
  multi.zadd(PROCESSING_PREFIX + payload_obj.queue_name, Date.now(), payload_obj.uuid)
  multi.zadd(PROGRESS_PREFIX + payload_obj.queue_name, 0, payload_obj.uuid)
  multi.exec();
}

function completeJob(payload_obj) {
    console.error("completeJob")
    multi = client.multi();
    multi.zrem(DISPATCHED_PREFIX + payload_obj.queue_name, payload_obj.uuid)
    multi.zrem(PROCESSING_PREFIX + payload_obj.queue_name, payload_obj.uuid)
    multi.zrem(PROGRESS_PREFIX + payload_obj.queue_name, payload_obj.uuid)
    multi.hset(payload_obj.uuid, "json", JSON.stringify(payload_obj));
    multi.exec();
}

function workerIdle(message_obj) {
    console.error("workerIdle")
    var d = new Date(message_obj.created_at)
    var timestamp = d.valueOf()
    client.zadd(WORKERS_IDLE_PREFIX + message_obj.payload.queue_name, "CH", parseFloat(timestamp), message_obj.service_uuid)
}

function workerProgress(payload_obj) {
    console.error("workerProgress")
    client.zadd(PROGRESS_PREFIX + payload_obj.queue_name, payload_obj.progress, payload_obj.uuid)
}

function isDate(str) {
    var d = new Date(str)
    if ( Object.prototype.toString.call(d) === "[object Date]" ) {
        // it is a date
        if ( isNaN( d.valueOf() ) ) {  // d.getTime() could also work
            return false
        }
        else {
            return true
        }
    }
    else {
        return false
    }
}

function purgeExpiredWorkers(queue_name, expired_at) {
    console.error("purgeExpiredWorkers")
    client.zremrangebyscore(WORKERS_IDLE_PREFIX + queue_name, "-inf", expired_at, function(err, res) {
        console.error("listWorkerQueue", res)
    })
}

function listWorkerQueue(payload_obj) {
    console.error("listWorkerQueue")
    var n = Date.now()
    var from, to
    if (payload_obj.hasOwnProperty('later_than')) {
        if (payload_obj.later_than === "-inf") {
            from = '-inf'
        } else {
            if (isDate(payload_obj.later_than)) {
                from = parseFloat(new Date(payload_obj.later_than).valueOf())
            } else {
                from = '-inf'
            }
        }

    } else {
        from = n - IDLE_EXPIRY_PERIOD
    }
    if (payload_obj.hasOwnProperty('earlier_than')) {
        if (payload_obj.earlier_than === "+inf") {
            to = '+inf'
        } else {
            if (isDate(payload_obj.earlier_than)) {
                to = parseFloat(new Date(payload_obj.earlier_than).valueOf())
            } else {
                to = '+inf'
            }
        }
    } else {
        to = '+inf'
    }
    client.zrangebyscore(WORKERS_IDLE_PREFIX + payload_obj.queue_name, from, to, function(err, res) {
        console.error("listWorkerQueue", res)
    })
}

function listJobQueue(payload_obj) {
    console.error("listJobQueue")
    client.lrange(JOB_QUEUE_PREFIX + payload_obj.queue_name, 0, -1, function(err, res) {
        console.error("listJobQueue", res);
    })
}

function listLeasedQueue(payload_obj) {
    console.error("listLeasedQueue")
    client.zrange(DISPATCHED_PREFIX + payload_obj.queue_name, 0, -1, "withscores", function(err, res) {
        console.error("listLeasedQueue", res);
    })
}

function listProgressQueue(payload_obj) {
    console.error("listProgressQueue")
    client.zrange(PROCESSING_PREFIX + payload_obj.queue_name, 0, -1, "withscores", function(err, res) {
        console.error("listProgressQueue", res);
    })
}



function reassignStalled() {

}

function tickResponse(payload_obj) {
    console.error("tickResponse")
}

function stdoutMessage(topic, payload_obj) {
    process.env.TOPIC = topic
    process.env.PAYLOAD = JSON.stringify(payload_obj);
    process.env.CREATED_AT = new Date().toISOString();
    process.stdout.write(expandenv(messageTemplate) + "\n");
}
