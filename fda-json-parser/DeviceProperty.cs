namespace fda_json_parser
{
    public class DeviceProperty
    {
        string keyName;
        object value;
        string colName;
        /// <summary>
        /// Constructor for when the column name is different from the key name
        /// </summary>
        /// <param name="keyName">name of the key from the json data</param>
        /// <param name="value">value of the key</param>
        /// <param name="colName">name of the column</param>
        public DeviceProperty(string keyName, object value, string colName)
        {
            this.keyName = keyName;
            this.value = value;
            this.colName = colName;
        }
        /// <summary>
        /// Constructor for when the column name is the same as the key name
        /// </summary>
        /// <param name="keyName">name of the key and column</param>
        /// <param name="value">value of the key</param>
        public DeviceProperty(string keyName, object value)
        {
            this.keyName = keyName;
            this.value = value;
            this.colName = keyName;
        }
        /// <summary>
        /// Getter method for value
        /// </summary>
        /// <returns>value of key</returns>
        public object GetValue()
        {
            return this.value;
        }
        /// <summary>
        /// Getter method for column name
        /// </summary>
        /// <returns>column name</returns>
        public string GetColumnName()
        {
            return this.colName;
        }
    }
}