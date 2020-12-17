local popped_value = redis.call('HGET', KEYS[1], KEYS[2])
redis.call('HDEL', KEYS[1], KEYS[2])
return popped_value
