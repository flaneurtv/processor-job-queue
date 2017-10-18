local jobs = redis.call("ZREVRANGEBYSCORE", KEYS[2], tonumber(ARGV[1]), "+inf")

for index, job_uuid in ipairs(jobs) do
  redis.call("ZREM", KEYS[2], job_uuid)
  redis.call("RPUSH", KEYS[1], job_uuid)
end
