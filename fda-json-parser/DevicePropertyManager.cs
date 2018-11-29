using System;
using System.Collections.Generic;
using System.Text;

namespace fda_json_parser
{
    public class DevicePropertyManager
    {
        /// <summary>
        /// Change the key name to match its column if the key has an illegal name or return the provided name
        /// </summary>
        /// <param name="queryName">name of the current query pair</param>
        /// <param name="name">name of the key to be tested</param>
        /// <param name="subQueryName">name of the query section (for device sizes)</param>
        /// <returns></returns>
        public static string getValidName(string queryName, string name, string subQueryName = "")
        {
            switch (queryName)
            {
                case "device_sizes":
                    switch (name)
                    {
                        case "text":
                            return "size_text";
                        case "type":
                            return "size_type";
                        case "value":
                            return "size_value";
                        default:
                            return name;
                    }
                case "identifiers":
                    switch (name)
                    {
                        case "id":
                            return "identifier_id";
                        case "type":
                            return "identifier_type";
                        default:
                            return name;
                    }
                case "storage":
                    switch (name)
                    {
                        case "value":
                            return subQueryName + "_value";
                        case "unit":
                            return subQueryName + "_unit";
                        case "type":
                            return "storage_type";
                        default:
                            return name;
                    }
                default:
                    return name;
            }
        }
    }
}
