using NRedisStack;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using System.Diagnostics;

namespace checkandset{
    public class RedisJSONCheckAndSetLua{
        static string luaStringSimple = "local workingVersionID = redis.call('JSON.RESP', KEYS[1], '$.versionID') if not workingVersionID then workingVersionID = 1 else if workingVersionID[1]..'' == ARGV[2]..'' then workingVersionID = ARGV[2] + 1 else if not tonumber(workingVersionID) then workingVersionID = workingVersionID[1] end return tonumber(workingVersionID) end end if redis.call('EXISTS', KEYS[1]) == 0 then redis.call('JSON.SET', KEYS[1], '$', '{\"val\": ' .. ARGV[1] .. '}') end redis.call('JSON.SET', KEYS[1], '$.versionID', workingVersionID) redis.call('JSON.SET', KEYS[1], '$.val', ARGV[1]) if not tonumber(workingVersionID) then workingVersionID = workingVersionID[1] end return tonumber(workingVersionID)";
        //This next LUA script accepts a JSON path as the ARGV[2] argument -which will be used as the destination for the value provided as ARGV[1] ARGV[3] now contains the versionID
        static string luaStringWithPath = "local suppliedJSONPath=ARGV[1] local suppliedJSONValue=ARGV[2] local suppliedVersionID=ARGV[3] local workingVersionID = redis.call('JSON.RESP', KEYS[1], '$.versionID') if not workingVersionID then workingVersionID = 1 else if workingVersionID[1]..'' == suppliedVersionID..'' then workingVersionID = tonumber(suppliedVersionID) + 1 else if not tonumber(workingVersionID[1]) then workingVersionID = tonumber(workingVersionID[1]) end return workingVersionID end end if redis.call('EXISTS', KEYS[1]) == 0 then redis.call('JSON.SET', KEYS[1], '$', '{\"'..suppliedJSONPath..'\": ' .. suppliedJSONValue .. '}') else local workingObject = redis.call('JSON.GET', KEYS[1]) redis.call('JSON.SET', KEYS[1], '$', workingObject) local outcome=redis.call('JSON.SET', KEYS[1], '$.'..suppliedJSONPath, suppliedJSONValue) if not outcome then return {'JSON API not HAPPY - did you try to use a new path with more than 1 level deep from $? ie: town.small'} end end redis.call('JSON.SET', KEYS[1], '$.versionID', workingVersionID) if not tonumber(workingVersionID) then workingVersionID = tonumber(workingVersionID[1]) end return workingVersionID";

        // "local suppliedVersionID=ARGV[3] local suppliedJSONPath=ARGV[1] local suppliedJSONValue=ARGV[2] local workingVersionID = redis.call('JSON.RESP', KEYS[1], '$.versionID') if not workingVersionID then workingVersionID = 1 else if workingVersionID[1]..'' == suppliedVersionID..'' then workingVersionID = tonumber(suppliedVersionID) + 1 else if not tonumber(workingVersionID[1]) then workingVersionID = tonumber(workingVersionID[1]) end return workingVersionID end end if redis.call('EXISTS', KEYS[1]) == 0 then redis.call('JSON.SET', KEYS[1], '$', '{\"'..suppliedJSONPath..'\": ' .. suppliedJSONValue .. '}') else local workingObject = redis.call('JSON.GET', KEYS[1]) redis.call('JSON.SET', KEYS[1], '$', workingObject) redis.call('JSON.SET', KEYS[1], '$.'..suppliedJSONPath, suppliedJSONValue) end redis.call('JSON.SET', KEYS[1], '$.versionID', workingVersionID) if not tonumber(workingVersionID) then workingVersionID = tonumber(workingVersionID[1]) end return workingVersionID";
        static string luaSimpleSHA = "TBD";
        static string luaPathSHA = "TBD";        
        static string luaSimpleSHAKeyName = "checkandset1:RedisJSONCheckAndSetLuaSimple";
        static string luaPathSHAKeyName = "checkandset1:RedisJSONCheckAndSetLuaPath";
        public static void Main(string[] args){
            string redisEndpoint = "searchme.southcentralus.redisenterprise.cache.azure.net:10000";
            //redisEndpoint = "redis-14154.homelab.local:14154";
            var redisOptions = ConfigurationOptions.Parse(redisEndpoint); // edit to suit
            redisOptions.Password = "n3Ce6xxqrNy95fg8L7paazrvIX6zkFNAZZqGNhHcrhk="; //for ACRE connections use Access Key            
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisOptions);
            setupSHAValues(redis);
            
            Console.WriteLine("This program will use Check and Set logic to modify a key in Redis");
            Console.WriteLine("The key is of type JSON and requires that you have the JSON module installed");
            Console.WriteLine("A LUA script handles updating the key with a new value  when the versionID is correct (in a simple, fixed path within the JSON object)");
            Console.WriteLine("If the version ID is a mismatch, the program will not update the value in the key");
            Console.WriteLine("(A future version will utilize JSON.MERGE to reduce complexity - once it is available in ACRE)");            
            
            /*
            * The next block of code calls a LUA script that accepts a path and a JSON value
            * the block of code manages the JSON keyNAME used and deletes it before each attempt
            * The Script assigns the JSON object the value at the path given
            * It also returns an updated versionID
            * It repeats the exercise with a variety of json values to see what
            * Breaks the LUA script
            */
            string jsonPATH = "test.path.1";
            string jsonValue = "";
            Boolean shouldDeleteKey = true;
            testJSONEscapingWithPath(redis,jsonPATH,jsonValue,shouldDeleteKey);
            jsonValue = "{\"tryme1\": \"a simple string with spaces\"}";
            testJSONEscapingWithPath(redis,jsonPATH,jsonValue,shouldDeleteKey);
            jsonValue = "{\"tryme2\": 1234567}";
            testJSONEscapingWithPath(redis,jsonPATH,jsonValue,shouldDeleteKey);
            jsonValue = "{\"tryme3\": {\"nested1\": \"a nested simple string with spaces\"}}";
            testJSONEscapingWithPath(redis,jsonPATH,jsonValue,shouldDeleteKey);
            jsonValue = "{\"tryme4\": {\"nestedPeers1\": \"a nested simple string with spaces\", \"nestedPeers2\": \"a second nested simple string with spaces\"}}";
            testJSONEscapingWithPath(redis,jsonPATH,jsonValue,shouldDeleteKey);
            jsonValue = "{\"usingArray1\": [{\"nested1\": \"a nested simple string with spaces\"},{\"nested2\": \"a second nested simple string with spaces\"}]}";
            testJSONEscapingWithPath(redis,jsonPATH,jsonValue,shouldDeleteKey);
            jsonValue = "{\"nestingArrayInNestedKey1\": {\"vsl\": [{\"nested1\": \"a nested simple string with spaces\"},{\"nested2\": \"a second nested simple string with spaces\"}]}}";
            testJSONEscapingWithPath(redis,jsonPATH,jsonValue,shouldDeleteKey);   
            Stopwatch duration = Stopwatch.StartNew();
            jsonValue = "{\"nestingArrayInNestedKey2\": {\"multiValue\": [{\"nested1\": \"a nested simple string with spaces\",\"phone\": \"712-919-8999\"},{\"nested2\": \"a second nested simple string with spaces\",\"phone\": \"212-921-8239\"}]}}";
            testJSONEscapingWithPath(redis,jsonPATH,jsonValue,shouldDeleteKey);   
            duration.Stop();
            Console.WriteLine(">> Deleting, Setting, and then Reading Check and Set Value (3 remote operations) took: "+duration.Elapsed); 
            
            //This time do not delete the JSONKEY (update it instead):
            jsonValue = "{\"NEWSTUFF\": {\"stuff1\": \"goodStuff\"}}";
            string newPath = "val";
            testJSONEscapingWithPath(redis,newPath,jsonValue,false);   
            
            //reset key:
            //testJSONEscapingWithPath(redis,jsonPATH,jsonValue,shouldDeleteKey);   
            
            //Hit another new simple PATH:
            jsonValue = "{\"SimpleNumber\": 655}";
            newPath = "property";
            testJSONEscapingWithPath(redis,newPath,jsonValue,false);   

            //Fail with too deeo a PATH:
            jsonValue = "{\"bigone\": 999995990095}";
            newPath = "level.two";
            testJSONEscapingWithPath(redis,newPath,jsonValue,false);   
            Console.WriteLine("\n^^^^ THAT WAS A DELIBERATE FAILURE CAUSED BY TOO DEEP A PATH ^^^^\n");

            //this faster version uses the hard-coded keyname: 'hardCodedKeyname'
            testCASVersion(redis);
            Console.WriteLine("\n\n NOTE That to be even faster than this "+
            "- wherever there are multiple Redis operations,"+
            " they should be placed into a pipeline and executed"+
            " in a single network call\n");
            
            doJSONMGET(redis); //test JSON.MGET from c#
        }

// functions 
// do not initiate a connection object, but reuse a shared one to enable efficiency
        private static void setupSHAValues(ConnectionMultiplexer redis){
            IDatabase redisDB = redis.GetDatabase();
            //check to see if our LUASHA value exists under the expected keyname:
            var tempSHA = redisDB.StringGet(luaSimpleSHAKeyName);
            if(null==(string?)tempSHA){
                //load the LUA Script ahead of time and store the SHA value
                Object[] oa = new Object[]{"LOAD",luaStringSimple};
                luaSimpleSHA = redisDB.Execute("SCRIPT",oa).ToString();
                redisDB.StringSet(luaSimpleSHAKeyName,luaSimpleSHA);
                Console.WriteLine("New SHA value for LUA script is: "+luaSimpleSHA);
            }else{
                Console.WriteLine("Found stored SHA value for LUA script: "+tempSHA);
                luaSimpleSHA=tempSHA.ToString();
            }
            long versionID = 0;
            //repeat SHA retrieval for version with dynamic PATH:
            tempSHA = redisDB.StringGet(luaPathSHAKeyName);
            if(null==(string?)tempSHA){
                //load the LUA Script ahead of time and store the SHA value
                Object[] oa = new Object[]{"LOAD",luaStringWithPath};
                luaPathSHA = redisDB.Execute("SCRIPT",oa).ToString();
                redisDB.StringSet(luaPathSHAKeyName,luaPathSHA);
                Console.WriteLine("New SHA value for LUA script is: "+luaPathSHA);
            }else{
                Console.WriteLine("Found stored SHA value for LUA script: "+tempSHA);
                luaPathSHA=tempSHA.ToString();
            }
        }
        private static void doJSONMGET(ConnectionMultiplexer redis){
            IDatabase redisDB = redis.GetDatabase();
            object[] oArray =  new object[] { "jsonEscapePathTest1", "hardCodedKeyname","versionID" };
            var res = redisDB.Execute("JSON.MGET",oArray, CommandFlags.None);
            Console.WriteLine("Response from JSON.MGET == \n");
            string[] sResult = (string[]) res;
            for(int x= 0; x< sResult.Length;x++){
                Console.Write("$.versionID for "+oArray[x]+" is "+sResult[x]+"\n");
            }
            oArray =  new object[] { "jsonEscapePathTest1", "hardCodedKeyname","val" };
            res = redisDB.Execute("JSON.MGET",oArray, CommandFlags.None);
            Console.WriteLine("\nResponse from JSON.MGET == \n");
            sResult = (string[]) res;
            for(int x= 0; x< sResult.Length;x++){
                Console.Write("$.val for "+oArray[x]+" is "+sResult[x]+"\n");
            }
        }

        //from LUA script
        //local suppliedJSONPath=ARGV[1] 
        //local suppliedJSONValue=ARGV[2] 
        //local suppliedVersionID=ARGV[3] 
        private static void testJSONEscapingWithPath(ConnectionMultiplexer redis, string jsonPath, string jsonValue,Boolean shouldDeleteKey){
            Console.WriteLine("\n\n*****************   ************************    *************\n\n"+
             "testJSONEscapingWithPath called with: "+jsonPath+" and "+jsonValue+" and "+shouldDeleteKey);
            string keyName = "jsonEscapePathTest1";
            long versionID = 0;
            long newVersionID = 0;
            IDatabase redisDB = redis.GetDatabase();
            var redisJSON = redisDB.JSON();

            // preparation for the test - delete any old key with the hard-coded name 
            if(shouldDeleteKey){
                redisDB.KeyDelete(keyName); // result doesn't matter 
            }else{
                var jsonAPIResponse = redisJSON.Get(key: keyName,path: "versionID");//no other args means just value returned 
                Console.WriteLine("\n\tjsonAPIResponse == "+jsonAPIResponse);
                versionID = (long)jsonAPIResponse;
            }
            
            var s = "";
            try{
                var v = redisDB.Execute("EVALSHA",new Object[]{luaPathSHA,1,keyName,jsonPath,jsonValue,versionID}).ToString();
                if(null!=v){
                    s=v;
                }
                s = s.Replace('[', ' ');
                s = s.Replace(']', ' ');
                s = s.Trim();
                Console.WriteLine("\n\tResponse from Check and Set lua call: " + s);
                newVersionID = long.Parse(s);
                var jsonObjectInRedis = redisJSON.Get(keyName); 
                Console.WriteLine("\n JSON object: "+keyName+" in Redis now looks like this:\n\n"+jsonObjectInRedis);
            }
            catch (Exception ex){
                Console.WriteLine("ERROR returned value from LUA = " + ex.Message);
                newVersionID = -1;
            }
        }

        // it assumes that the client code context creates a connection and reuses it
        // this avoids wasting time recreating a connection with each call
        // It first deletes any object with the test key name and then updates it
        // It later fetches the new value - all of these operations use the same connection 
        private static void testCASVersion(ConnectionMultiplexer redis){
            Console.WriteLine("\n\n************************\n\nTest of reusing RedisConnection across multiple methods ...");
            string keyName = "hardCodedKeyname";
            string newValue = "200987654";
            long originalVersionID = 0;
            long wrongVersionID = 9999;
            string wrongValue = "999999";
            IDatabase redisDB = redis.GetDatabase();
            // preparation for the test - delete any old key with the hard-coded name 
            var deleteResult = redisDB.KeyDelete(keyName);
            Stopwatch duration = Stopwatch.StartNew();
            var newVersionID = CheckAndSetWithLuaJSON(keyName,newValue,originalVersionID,redisDB); 
            duration.Stop();
            validateReturnedVersionID(originalVersionID,newVersionID);
            Console.WriteLine(">> Updating Check and Set Value took: "+duration.Elapsed); 
            duration = Stopwatch.StartNew();
            var casVal =  GetCheckAndSetJSONValue(keyName, redisDB);
            duration.Stop();
            Console.WriteLine(">> Fetching Check and Set Value took: "+duration.Elapsed); 
            Console.WriteLine("\tAfter update -->   value of " + keyName + " = " +casVal);
            Console.WriteLine("\nDeliberately passing the wrong versionID to the CheckAndSetWithLuaJSONFaster() method.");
            Console.WriteLine("\nTrying to set the value to: "+wrongValue);
            duration = Stopwatch.StartNew();
            var returnedVersionID = CheckAndSetWithLuaJSON(keyName,wrongValue,wrongVersionID,redisDB); 
            duration.Stop();
            Console.WriteLine(">> Updating Check and Set Value took: "+duration.Elapsed); 
            duration = Stopwatch.StartNew();
            casVal =  GetCheckAndSetJSONValue(keyName, redisDB);
            duration.Stop();
            Console.WriteLine(">> Fetching Check and Set Value took: "+duration.Elapsed); 
            Console.WriteLine("\t\tAfter deliberately failed update -->   value of " + keyName + " = " +casVal);    
            validateReturnedVersionID(wrongVersionID,returnedVersionID);
        }


        //Only prints a message if the versionID provided to the checkAndSet is incorrect:
        private static void validateReturnedVersionID(long utilizedVersionID,long returnedVersionID){
            if (returnedVersionID != utilizedVersionID + 1)
            {
                Console.WriteLine("\t[[[[[[{ ALERT }]]]]]]\n\tValue not updated! You need to refresh your local copy - the expected versionID is: " + returnedVersionID);
            }
        }

        private static long CheckAndSetWithLuaJSON(string keyName, string value, long versionID,IDatabase redisDB){
            Console.WriteLine($"{Environment.NewLine}CheckAndSetWithLuaJSONFaster() called with args: {keyName}, {value}, {versionID}  ...");
            long newVersionID = 0;
            // Fire the lua script that adds the new value and retrieves the VersionID:
            //for kicks I demonstrate using EVALSHA instead of passing the script
            string s = "";
            try
            {
                var v = redisDB.Execute("EVALSHA",new Object[]{luaSimpleSHA,1,keyName,value,versionID}).ToString();
                if(null!=v){
                    s=v;
                }
                s = s.Replace('[', ' ');
                s = s.Replace(']', ' ');
                s = s.Trim();
                Console.WriteLine("\n\tResponse from Check and Set lua call: " + s);
                newVersionID = long.Parse(s);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR returned value from LUA = " + ex.Message);
                newVersionID = -1;
            }
            return newVersionID;
        }


        // This method reuses an existing connection to Redis when it is called:
        //for more JSON API information see: https://github.com/redis/NRedisStack/blob/master/Examples/BasicJsonExamples.md
        private static string GetCheckAndSetJSONValue(string keyName,IDatabase db){
            //ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisOptions);
            //IDatabase db = redis.GetDatabase();
            IJsonCommands json = db.JSON();
            
            string result = "NULL Returned --This is not expected.";

            RedisResult[] lResult = json.Resp(new RedisKey(keyName), "$.val");
            if (null == lResult)
            {
                Console.WriteLine("Path == $.val on json object " + keyName + " is null");
                Console.WriteLine("Exists: " + keyName + " returns: " + db.KeyExists(keyName));
            }
            else
            {
                var r = lResult[0].ToString();
                if(null!=r){
                    result = r;
                }
                Console.WriteLine("\nGetCheckAndSetJSONValue() fetches : " + result);
            }
            return result;
        }
    }
}