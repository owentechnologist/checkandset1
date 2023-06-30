This command line program uses LUA scripting server-side to implement check and set behavior in Redis.

It also uses a JSON object as the wrapper for the protected value.

To run: 
- make sure you have dotnet 7 installed on your system to support C# execution
- install NRedisStack 
```
dotnet add package NRedisStack
```
- modify the Program.cs code so that the configuration details for Redis match your instance of Redis
- make sure you are running a version of Redis that has the JSON module installed
- execute 
```
dotnet run
```
and follow the prompts as in the following example execution:

```
owentaylor@Owens-MacBook-Pro checkandset1 % dotnet run
This program will use Check and Set logic to modify a key in Redis
The key is of type JSON and requires that you have the JSON module installed
A LUA script handles updating the key with a new value when the versionID is correct (in a simple, fixed path within the JSON object)
If the version ID is a mismatch, the program will not update the value in the key

A Gold-plated version of this program would accept a JSON path as well as the value.
(A future version will utilize JSON.MERGE to reduce complexity - once it is available in ACRE)

What keyName do you wish to use?
mykey1
What new Value do you wish to set? [use an integer]
567
What versionID do you want to use? [use an integer]
3

CheckAndSetWithLuaJSON() called with args: mykey1, 567, 3  ...

        Response from Check and Set lua call: 4

getWithJedisPooled() fetches : 567
        After update -->   value of mykey1 = 567

 This versionID will be expected for future mutations --> 4
 ```

 The lua logic is somewhat dense and includes adding a timestamp and an old copy of the value of interest to be stored in the JSON object - which enables 1-level rollback behavior.

 Here is the LUA code pretty printed for humans:
  ```
local changeTime = redis.call('TIME')
local putold = redis.call('JSON.RESP', KEYS[1], '$.val')
local workingVersionID = redis.call('JSON.RESP', KEYS[1], '$.versionID')

if not workingVersionID then
    workingVersionID = 1
else
    if workingVersionID[1]..'' == ARGV[2]..'' then
        workingVersionID = ARGV[2] + 1
    else
        if not tonumber(workingVersionID) then
            workingVersionID = workingVersionID[1]
        end
        return tonumber(workingVersionID)
    end
end

if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('JSON.SET', KEYS[1], '$', '{\"val\": ' .. ARGV[1] .. '}')
end

if putold then
    redis.call('JSON.SET', KEYS[1], '$.oldval', putold[1])
end

redis.call('JSON.SET', KEYS[1], '$.timestamp', changeTime[1] .. ':' .. changeTime[2])
redis.call('JSON.SET', KEYS[1], '$.versionID', workingVersionID)
redis.call('JSON.SET', KEYS[1], '$.val', ARGV[1])

if not tonumber(workingVersionID) then
    workingVersionID = workingVersionID[1]
end

return tonumber(workingVersionID)
```

