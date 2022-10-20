local key = tostring(KEYS[1])
local cost = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local period = tonumber(ARGV[3]) * 1000 * 1000 -- converting to microseconds
local current_time = redis.call('TIME')
local current_time_microseconds = tonumber(current_time[1] .. current_time[2])
local trim_time = current_time_microseconds - period

-- Remove all the requests that are older than the window
redis.call('ZREMRANGEBYSCORE', key, 0, trim_time)

-- Check number of requests first
if cost < 1 then
    -- nothing to do, the user didn't ask for any tokens
    return "0"
end

-- Get the number of requests in the current window
local used_tokens = redis.call('ZCARD', key)

-- If the number of requests is greater than the max requests we hit the limit
if (used_tokens + cost) > tonumber(rate) then
    return "0"
end

local members = ""
for i = 1, cost, 1 do
    local member = current_time_microseconds .. i
    redis.call('ZADD', key, current_time_microseconds, member) -- can this be batched in a single ZADD call?
    members = members .. member .. ","
end

redis.call('EXPIRE', key, period)

members = members:sub(1, -2) -- remove the last comma
return members
