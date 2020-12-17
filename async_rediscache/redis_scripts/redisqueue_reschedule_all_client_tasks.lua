local queue_size = redis.call("LLEN", KEYS[2])
local client_tasks = redis.call("LRANGE", KEYS[2], 0, queue_size)

-- For Lua version compatibility, try picking the right version of unpack
local unpack_function
if unpack == nil then
  unpack_function = table.unpack
else
  unpack_function = unpack
end

if queue_size > 0 then
  redis.call("RPUSH", KEYS[1], unpack_function(client_tasks))
end

redis.call("DEL", KEYS[2])
return queue_size
