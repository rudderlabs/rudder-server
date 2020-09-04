function getAlphaChar()
    selection = math.random(1, 3)
    if selection == 1 then return string.char(math.random(65, 90)) end
    if selection == 2 then return string.char(math.random(97, 122)) end
    return string.char(math.random(48, 57))
end


function getRandomString(length)
           length = length or 1
                if length < 1 then return nil end
                local array = {}
                for i = 1, length do
                    array[i] = getAlphaChar()
                end
                return table.concat(array)
end

function removeTrailingSlash(s)
  return (s:gsub("(.-)/*$", "%1"))
end



wrk.method = "POST"
wrk.headers["Authorization"] = "Basic <base_64_encoded_writekey_colon>"
wrk.headers["Content-Type"] = "application/json"
wrk.body = '{ "batch": [ { "anonymousId": "anon_id", "channel": "android-sdk", "messageId": "1566904375469-1baf3108-647a-4f20-9867-3072056a07f5", "context": { "app": { "build": "1", "name": "RudderAndroidClient", "namespace": "com.rudderlabs.android.sdk", "version": "1.0" }, "device": { "id": "49e4bdd1c280bc00", "manufacturer": "Google", "model": "Android SDK built for x86", "name": "generic_x86" }, "locale": "en-US", "network": { "carrier": "Android" }, "screen": { "density": 420, "height": 1794, "width": 1080 }, "library": { "name": "com.rudderstack.android.sdk.core" }, "traits": { "anonymousId": "50e4bdd1c280bc00" }, "user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)" }, "event": "Demo Track", "integrations": { "All": true }, "properties": { "label": "Demo Label", "category": "Demo Category", "value": 5 }, "type": "track", "originalTimestamp": "2019-08-12T05:08:30.909Z", "sentAt": "2019-08-12T05:08:30.909Z" } ] }'


-- add a random string to the original request path.
request = function()
    local body = wrk.body:gsub("anon_id", getRandomString(20))
    return wrk.format(wrk.method, wrk.path, wrk.headers, body)
end
