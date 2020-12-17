local queue_size = redis.call("LLEN", KEYS[2])
local client_tasks = redis.call("LRANGE", KEYS[2], 0, queue_size)

if queue_size > 0 then
  redis.call("LPUSH", KEYS[1], unpack(client_tasks))
end

redis.call("DEL", KEYS[2])
return queue_size
