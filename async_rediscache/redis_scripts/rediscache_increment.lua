local value = redis.call('HGET', KEYS[1], KEYS[2]);
if not value then value = "i|0" end

local get_prefix = function (redis_value)
    local prefix_end = redis_value:find("|")
    return prefix_end, redis_value:sub(1, prefix_end-1)
end

local value_prefix_end, value_prefix = get_prefix(value)
if not value_prefix_end then
    return string.format("ValueError|received malformed value from keys %s %s: `%s`", KEYS[1], KEYS[1], value)
end

local increment = ARGV[1]
local incr_prefix_end, incr_prefix = get_prefix(increment)
if not incr_prefix_end then
    return string.format("ValueError|received malformed increment value: `%s`", increment)
end

local valid_prefixes = "if"
local valid_values = valid_prefixes:match(value_prefix) and valid_prefixes:match(incr_prefix)
if not valid_values then
    return string.format("TypeError|cannot increment value `%s` with `%s`.", value, ARGV[1])
end

local new_value = value:sub(value_prefix_end+1) + increment:sub(incr_prefix_end+1)
local result
if incr_prefix..value_prefix == "ii" then
    result = string.format("i|%d", new_value)
else
    result = string.format("f|%s", tostring(new_value))
end

redis.call("HSET", KEYS[1], KEYS[2], result)
return result
