local number_rescheduled = redis.call("LREM", KEYS[2], 1, ARGV[1])
if number_rescheduled == 0 then
    return redis.error_reply("Task not found in pending tasks queue.")
end

redis.call("LPUSH", KEYS[1], ARGV[1])
