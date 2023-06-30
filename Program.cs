using NRedisStack;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace checkandset{
    public class RedisJSONCheckAndSetLua{

        public static void Main(string[] args){
            var redisOptions = ConfigurationOptions.Parse("redis-14154.homelab.local:14154"); // edit to suit
            //redisOptions.Password = "yourPassword"; //for ACRE connections use key
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisOptions);
            
            Console.WriteLine("This program will use Check and Set logic to modify a key in Redis");
            Console.WriteLine("The key is of type JSON and requires that you have the JSON module installed");
            Console.WriteLine("A LUA script handles updating the key with a new value  when the versionID is correct (in a simple, fixed path within the JSON object)");
            Console.WriteLine("If the version ID is a mismatch, the program will not update the value in the key");
            Console.WriteLine("\nA Gold-plated version of this program would accept a JSON path as well as the value.");                                    
            Console.WriteLine("(A future version will utilize JSON.MERGE to reduce complexity - once it is available in ACRE)");            
            Console.WriteLine("\nWhat keyName do you wish to use?");
            string keyName = "casjsonkey";
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
            Console.WriteLine("What versionID do you want to use? [use an integer]");
            long versionID = 0;
            s = Console.ReadLine();
            if(null!=s){
                versionID = long.Parse(s);
            }
            var newVersionID = CheckAndSetWithLuaJSON(keyName,newValue,versionID,redis); 
            
            Console.WriteLine("\tAfter update -->   value of " + keyName + " = " + GetCheckAndSetJSONValue(keyName, redis));
            Console.WriteLine("\n This versionID will be expected for future mutations --> "+ newVersionID);
            if (newVersionID != versionID + 1)
            {
                Console.WriteLine("\t[[[[[[{ ALERT }]]]]]]\n\tValue not updated! You need to refresh your local copy - current versionID is: " + newVersionID);
            }
        }

        // This method is not performant as it creates a connection everytime it is called
        private static long CheckAndSetWithLuaJSON(string keyName, string value, long versionID,ConnectionMultiplexer redis){
            Console.WriteLine($"{Environment.NewLine}CheckAndSetWithLuaJSON() called with args: {keyName}, {value}, {versionID}  ...");
            //ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(redisOptions);
            IDatabase redisDB = redis.GetDatabase();
            long newVersionID = 0;
            // Create the LUA script string for our use:
            string luaString = "local changeTime = redis.call('TIME') local putold = redis.call('JSON.RESP', KEYS[1], '$.val') local workingVersionID = redis.call('JSON.RESP', KEYS[1], '$.versionID') if not workingVersionID then workingVersionID = 1 else if workingVersionID[1]..'' == ARGV[2]..'' then workingVersionID = ARGV[2] + 1 else if not tonumber(workingVersionID) then workingVersionID = workingVersionID[1] end return tonumber(workingVersionID) end end if redis.call('EXISTS', KEYS[1]) == 0 then redis.call('JSON.SET', KEYS[1], '$', '{\"val\": ' .. ARGV[1] .. '}') end if putold then redis.call('JSON.SET', KEYS[1], '$.oldval', putold[1]) end redis.call('JSON.SET', KEYS[1], '$.timestamp', changeTime[1] .. ':' .. changeTime[2]) redis.call('JSON.SET', KEYS[1], '$.versionID', workingVersionID) redis.call('JSON.SET', KEYS[1], '$.val', ARGV[1]) if not tonumber(workingVersionID) then workingVersionID = workingVersionID[1] end return tonumber(workingVersionID)";
                   // Create the List of keynames to pass along with the lua script:
            List<RedisKey> keynamesArray = new List<RedisKey>();
            keynamesArray.Add(keyName);
            List<RedisValue> values = new List<RedisValue>();
            values.Add(value);
            values.Add(versionID.ToString());
            // Fire the lua script that adds the new value and stores any old value in the rollback attribute and retrieves the VersionID:
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
                Console.WriteLine("\ngetWithJedisPooled() fetches : " + result);
            }
            return result;
        }
    }
}