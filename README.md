# processor-job-queue
A Redis job queue service processor, which communicates via json messages on stdin, stdout and stderr


add_job
multi = client.multi();
multi.hset(payload_obj.uuid, "json", JSON.stringify(payload_obj));
multi.lpush([JOB_QUEUE_PREFIX + payload_obj.queue_name, payload_obj.uuid]);
multi.exec();

assign_job
rpop
