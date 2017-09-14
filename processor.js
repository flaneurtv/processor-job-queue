var readline = require('readline');
var fs = require('fs');
var redis = require("redis"),
    client = redis.createClient({"host":"redis"});
const TICK_INTERVAL = 3000
const IDLE_EXPIRY_PERIOD = TICK_INTERVAL * 3
const MAX_LEASE_TIME = TICK_INTERVAL * 10
const JOB_QUEUE_PREFIX = "job_queue:"
const LEASED_QUEUE_PREFIX = "leased_queue:"
const PROGRESS_QUEUE_PREFIX = "progress_queue:"
const IDLE_QUEUE_PREFIX = "idle_queue:"

leaseJob_lua = 'local job_uuid = redis.call("RPOP", "' + JOB_QUEUE_PREFIX + '" .. KEYS[1])\n'
leaseJob_lua += 'redis.call("ZADD", "' + LEASED_QUEUE_PREFIX + '" .. KEYS[1], KEYS[2], job_uuid)\n'
leaseJob_lua += 'redis.call("ZADD", "' + PROGRESS_QUEUE_PREFIX + '" .. KEYS[1], 0, job_uuid)\n'
leaseJob_lua += 'redis.call("HSET", job_uuid, "leased_at", KEYS[2])\n'
leaseJob_lua += 'return redis.call("HGET", job_uuid, "json")'
leaseJob_sha = client.script('load', leaseJob_lua)

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
        json_obj = JSON.parse(line);
    } catch (e) {
        console.error('{"log_level": "error", "created_at": "' + new Date().toISOString() + '", "log_message": "payload is not valid JSON: ' + escapeQuotes(line) + '"}');
        return
    }
        switch(json_obj["topic"].replace(process.env.NAMESPACE_LISTENER + "/job_queue/","")) {
            case "add_job":
                addJob(json_obj.payload)
                break;
            case "lease_job":
                leaseJob(json_obj)
                break;
            case "complete_job":
                completeJob(json_obj.payload)
                break;
            case "worker_idle":
                workerIdle(json_obj)
                break;
            case "worker_progress":
                workerProgress(json_obj.payload)
                break;
            case "list_job_queue":
                listJobQueue(json_obj.payload)
                break;
            case "list_leased_queue":
                listLeasedQueue(json_obj.payload)
                break;
            case "list_progress_queue":
                listProgressQueue(json_obj.payload)
                break;
            case "list_worker_queue":
                listWorkerQueue(json_obj.payload)
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

function leaseJob(json_obj) {
    console.error("leaseJob", json_obj)
    leased_at = Date.now();
    client.eval(leaseJob_lua, 2, json_obj.payload.queue_name, leased_at, function(err, res) {
        console.error(err);
        console.error(res);
        job_obj = JSON.parse(res);
        job_obj.leased_at = leased_at;
        topic = json_obj.service_uuid + "/job_assignment"
        stdoutMessage(topic, job_obj);
    });
    /*
       We purge expired workers from the idle list for a particular queue_name,
       whenever a job is leased from this queue. Purging expired workers is not
       a necessity for proper operation. But in case number and UUIDs of
       workers change frequently e.g. due to elastic scaling of workers, it
       makes sense in the long run.
     */
    purgeExpiredWorkers(json_obj.payload.queue_name, Date.now() - IDLE_EXPIRY_PERIOD)
}

function completeJob(payload_obj) {
    console.error("completeJob")
    multi = client.multi();
    multi.zrem(LEASED_QUEUE_PREFIX + payload_obj.queue_name, payload_obj.uuid)
    multi.zrem(PROGRESS_QUEUE_PREFIX + payload_obj.queue_name, payload_obj.uuid)
    multi.hset(payload_obj.uuid, "json", JSON.stringify(payload_obj));
    multi.exec();
}

function workerIdle(message_obj) {
    console.error("workerIdle")
    var d = new Date(message_obj.created_at)
    var timestamp = d.valueOf()
    client.zadd(IDLE_QUEUE_PREFIX + message_obj.payload.queue_name, parseFloat(timestamp), message_obj.service_uuid)
}

function workerProgress(payload_obj) {
    console.error("workerProgress")
    client.zadd(LEASED_QUEUE_PREFIX + payload_obj.queue_name, Date.now(), payload_obj.uuid)
    client.zadd(PROGRESS_QUEUE_PREFIX + payload_obj.queue_name, payload_obj.progress, payload_obj.uuid)
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
    client.zremrangebyscore(IDLE_QUEUE_PREFIX + queue_name, "-inf", expired_at, function(err, res) {
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
    client.zrangebyscore(IDLE_QUEUE_PREFIX + payload_obj.queue_name, from, to, function(err, res) {
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
    client.zrange(LEASED_QUEUE_PREFIX + payload_obj.queue_name, 0, -1, "withscores", function(err, res) {
        console.error("listLeasedQueue", res);
    })
}

function listProgressQueue(payload_obj) {
    console.error("listProgressQueue")
    client.zrange(PROGRESS_QUEUE_PREFIX + payload_obj.queue_name, 0, -1, "withscores", function(err, res) {
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
