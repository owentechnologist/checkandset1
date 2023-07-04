using NRedisStack;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using System.Diagnostics;

namespace checkandset{
    public class RedisJSONCheckAndSetLua{
        static string luaString = "local workingVersionID = redis.call('JSON.RESP', KEYS[1], '$.versionID') if not workingVersionID then workingVersionID = 1 else if workingVersionID[1]..'' == ARGV[2]..'' then workingVersionID = ARGV[2] + 1 else if not tonumber(workingVersionID) then workingVersionID = workingVersionID[1] end return tonumber(workingVersionID) end end if redis.call('EXISTS', KEYS[1]) == 0 then redis.call('JSON.SET', KEYS[1], '$', '{\"val\": ' .. ARGV[1] .. '}') end redis.call('JSON.SET', KEYS[1], '$.versionID', workingVersionID) redis.call('JSON.SET', KEYS[1], '$.val', ARGV[1]) if not tonumber(workingVersionID) then workingVersionID = workingVersionID[1] end return tonumber(workingVersionID)";
        static string luaSHA = "TBD";
        static string luaSHAKeyName = "checkandset1:RedisJSONCheckAndSetLua";
        public static void Main(string[] args){
            string redisEndpoint = "jsonme.centralus.redisenterprise.cache.azure.net:10000";
            redisEndpoint = "redis-14154.homelab.local:14154";
            var redisOptions = ConfigurationOptions.Parse(redisEndpoint); // edit to suit
            //redisOptions.Password = "n9ENxshPYpXRUU1lon3qP6ANDJ41iITaAICViF90FwQ="; //for ACRE connections use Access Key
            
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisOptions);
            IDatabase redisDB = redis.GetDatabase();
            //check to see if our LUASHA value exists under the expected keyname:
            var psha = redisDB.StringGet(luaSHAKeyName);
            if(null==(string?)psha){
                //load the LUA Script ahead of time and store the SHA value
                Object[] oa = new Object[]{"LOAD",luaString};
                luaSHA = redisDB.Execute("SCRIPT",oa).ToString();
                redisDB.StringSet(luaSHAKeyName,luaSHA);
                Console.WriteLine("New SHA value for LUA script is: "+luaSHA);
            }else{
                Console.WriteLine("Found stored SHA value for LUA script: "+psha);
                luaSHA=psha.ToString();
            }
            
            Console.WriteLine("This program will use Check and Set logic to modify a key in Redis");
            Console.WriteLine("The key is of type JSON and requires that you have the JSON module installed");
            Console.WriteLine("A LUA script handles updating the key with a new value  when the versionID is correct (in a simple, fixed path within the JSON object)");
            Console.WriteLine("If the version ID is a mismatch, the program will not update the value in the key");
            Console.WriteLine("\nA Gold-plated version of this program would accept a JSON path as well as the value.");                                    
            Console.WriteLine("(A future version will utilize JSON.MERGE to reduce complexity - once it is available in ACRE)");            
            Console.WriteLine("\nWhat keyName do you wish to use?");
            string keyName = "TBD";
            var s = Console.ReadLine();
            if(null!=s){
                keyName = s;
            }
            Console.WriteLine("What new Value do you wish to set? [use an integer]");
            string newValue = "300";
            s = Console.ReadLine();
            if(null!=s){
                newValue = s;
            }
            Console.WriteLine("What versionID do you want to use? [use an integer]\nThe LUA script expects a 0 but accepts any integer value for new keys\n(and sets the versionID to 1)");
            long versionID = 0;
            s = Console.ReadLine();
            if(null!=s){
                versionID = long.Parse(s);
            }
            //Use System time on this client to see duration of this CheckAndSet behavior
            Stopwatch duration = Stopwatch.StartNew();
            var newVersionID = CheckAndSetWithLuaJSON(keyName,newValue,versionID,redis); 
            duration.Stop();
            Console.WriteLine(">> Updating Check and Set Value took: "+duration.Elapsed); 
            validateReturnedVersionID(versionID,newVersionID);

            duration = Stopwatch.StartNew();
            var casVal =  GetCheckAndSetJSONValue(keyName, redis);
            duration.Stop();
            Console.WriteLine(">> Fetching Check and Set Value took: "+duration.Elapsed); 
            Console.WriteLine("\tAfter update -->   value of " + keyName + " = " +casVal);
            
            Console.WriteLine("\n This versionID will be expected for future mutations of your key: "+keyName+" --> "+ newVersionID);

            //this faster version uses the hard-coded keyname: 'hardCodedKeyname'
            testFasterVersion(redis);
            Console.WriteLine("\n\n NOTE That to be even faster than this - wherever possible, operations should be placed into a pipeline and executed in a single network call");
        }

        //test faster version of method to get data:
        // it assumes that the client code context creates a connection and reuses it
        // this avoids wasting time recreating a connection with each call
        // It first deletes any object with the test key name and then updates it
        // It later fetches the new value - all of these operations use the same connection 
        // the time taken to initialize the now shared connection is not measured
        private static void testFasterVersion(ConnectionMultiplexer redis){
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
            var newVersionID = CheckAndSetWithLuaJSONFaster(keyName,newValue,originalVersionID,redisDB); 
            duration.Stop();
            validateReturnedVersionID(originalVersionID,newVersionID);
            Console.WriteLine(">> Updating Check and Set Value took: "+duration.Elapsed); 
            duration = Stopwatch.StartNew();
            var casVal =  GetCheckAndSetJSONValueFaster(keyName, redisDB);
            duration.Stop();
            Console.WriteLine(">> Fetching Check and Set Value took: "+duration.Elapsed); 
            Console.WriteLine("\tAfter update -->   value of " + keyName + " = " +casVal);
            Console.WriteLine("\nDeliberately passing the wrong versionID to the CheckAndSetWithLuaJSONFaster() method.");
            Console.WriteLine("\nTrying to set the value to: "+wrongValue);
            duration = Stopwatch.StartNew();
            var returnedVersionID = CheckAndSetWithLuaJSONFaster(keyName,wrongValue,wrongVersionID,redisDB); 
            duration.Stop();
            Console.WriteLine(">> Updating Check and Set Value took: "+duration.Elapsed); 
            duration = Stopwatch.StartNew();
            casVal =  GetCheckAndSetJSONValueFaster(keyName, redisDB);
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

        // This method is not performant as it creates a connection everytime it is called
        private static long CheckAndSetWithLuaJSON(string keyName, string value, long versionID,ConnectionMultiplexer redis){
            Console.WriteLine($"{Environment.NewLine}CheckAndSetWithLuaJSON() called with args: {keyName}, {value}, {versionID}  ...");
            //ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisOptions);
            IDatabase redisDB = redis.GetDatabase();
            long newVersionID = 0;
            
            // Create the List of keynames to pass along with the lua script:
            List<RedisKey> keynamesArray = new List<RedisKey>();
            keynamesArray.Add(keyName);
            List<RedisValue> values = new List<RedisValue>();
            values.Add(value);
            values.Add(versionID.ToString());
            // Fire the lua script that adds the new value and retrieves the VersionID:
            string s = "";
            try
            {
                var v = redisDB.ScriptEvaluate(luaString, keynamesArray.ToArray(), values.ToArray()).ToString();
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
        
        // This method is not performant as it creates a connection everytime it is called
        //for more JSON API information see: https://github.com/redis/NRedisStack/blob/master/Examples/BasicJsonExamples.md
        private static string GetCheckAndSetJSONValue(string keyName,ConnectionMultiplexer redis){
            //ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisOptions);
            IDatabase db = redis.GetDatabase();
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

// New Section of CODE where the functions 
// do not initiate a connection object, but reuse a shared one

        private static long CheckAndSetWithLuaJSONFaster(string keyName, string value, long versionID,IDatabase redisDB){
            Console.WriteLine($"{Environment.NewLine}CheckAndSetWithLuaJSONFaster() called with args: {keyName}, {value}, {versionID}  ...");
            long newVersionID = 0;
            // Fire the lua script that adds the new value and retrieves the VersionID:
            //for kicks I demonstrate using EVALSHA instead of passing the script
            string s = "";
            try
            {
                var v = redisDB.Execute("EVALSHA",new Object[]{luaSHA,1,keyName,value,versionID}).ToString();
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
        private static string GetCheckAndSetJSONValueFaster(string keyName,IDatabase db){
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