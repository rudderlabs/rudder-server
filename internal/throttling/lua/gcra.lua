--[[
To debug this script you can add these entries here in the script and then check the Redis server output:
redis.log(redis.LOG_NOTICE, "some label", some_variable)

For more information please refer to: https://redis.io/docs/manual/programmability/lua-debugging/
--]]

local rate_limit_key = KEYS[1]
local burst = ARGV[1]
local rate = ARGV[2]
local period = ARGV[3] * 1000 * 1000 -- converting to microseconds
local cost = tonumber(ARGV[4])
local emission_interval = period / rate
local increment = emission_interval * cost
local burst_offset = emission_interval * burst

-- Redis returns time as an array containing two integers: seconds of the epoch
-- time (10 digits) and microseconds (~5-6 digits). for convenience we need to
-- convert them to a floating point number. The resulting number is 16 digits,
-- bordering on the limits of a 64-bit double-precision floating point number.
-- Adjust the epoch to be relative to Jan 1, 2017 00:00:00 GMT to avoid floating
-- point problems. This approach is good until "now" is 2,483,228,799 (Wed, 09
-- Sep 2048 01:46:39 GMT), when the adjusted value is 16 digits.
local jan_1_2017 = 1483228800 * 1000 * 1000 -- in microseconds precision
local current_time = redis.call("TIME")
local microseconds = current_time[2]
while string.len(microseconds) < 6 do
    -- in case the microseconds part (i.e. current_time[2]) is less than 6 digits
    microseconds = "0" .. microseconds
end

local current_time_micro = tonumber(current_time[1] .. microseconds)
current_time = current_time_micro - jan_1_2017

local tat = redis.call("GET", rate_limit_key)
if not tat then
    tat = current_time
else
    tat = tonumber(tat)
end
tat = math.max(tat, current_time)

local new_tat = tat + increment
local allow_at = new_tat - burst_offset
local diff = current_time - allow_at
local remaining = diff / emission_interval
if remaining < 0 then
    local reset_after = tat - current_time
    local retry_after = diff * -1
    return {
        current_time_micro,
        0, -- allowed
        0, -- remaining
        tostring(retry_after),
        tostring(reset_after),
    }
end

local reset_after = new_tat - current_time
if reset_after > 0 then
    redis.call("SET", rate_limit_key, new_tat, "EX", math.ceil(reset_after))
end

local retry_after = -1
return { current_time_micro, cost, remaining, tostring(retry_after), tostring(reset_after) }
