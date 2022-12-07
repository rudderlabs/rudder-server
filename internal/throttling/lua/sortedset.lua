--[[
To debug this script you can add these entries here in the script and then check the Redis server output:
redis.log(redis.LOG_NOTICE, "some label", some_variable)

For more information please refer to: https://redis.io/docs/manual/programmability/lua-debugging/
--]]

local key = tostring(KEYS[1])
local cost = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local period = tonumber(ARGV[3]) * 1000 * 1000 -- converting to microseconds

-- Check number of requests first
if cost < 1 then
    -- nothing to do, the user didn't ask for any tokens
    return { 0, "" }
end

local current_time = redis.call('TIME')
local microseconds = current_time[2]
while string.len(microseconds) < 6 do
    -- in case the microseconds part (i.e. current_time[2]) is less than 6 digits
    microseconds = "0" .. microseconds
end

local current_time_micro = tonumber(current_time[1] .. microseconds)
local trim_time = current_time_micro - period

-- Remove all the requests that are older than the window
redis.call('ZREMRANGEBYSCORE', key, 0, trim_time)

-- Get the number of requests in the current window
local used_tokens = redis.call('ZCARD', key)

-- If the number of requests is greater than the max requests we hit the limit
if (used_tokens + cost) > tonumber(rate) then
    return { current_time_micro, "" }
end

-- seed needed to generate random members in case of collision
math.randomseed(current_time_micro)

local members = ""
for i = 1, cost, 1 do
    local added = 0
    local member = current_time_micro .. i

    while added == 0 do
        -- ZADD key score member
        -- current_time_micro is used for the "score" to sort the members in the sorted set
        -- "member" is the token representing the request and has to be unique (score does not have to be unique)
        added = redis.call('ZADD', key, current_time_micro, member)
        if added == 0 then
            -- the member already exists, we need to generate a new one
            -- the overhead for math.random is used only in case of collision
            member = current_time_micro .. i .. math.random(1, 1000000) -- using a string, no risk of truncation
        else
            members = members .. member .. ","
        end
    end
end

redis.call('EXPIRE', key, period)

members = members:sub(1, -2) -- remove the last comma
return { current_time_micro, members }
