namespace fda_json_parser
{
    public class DeviceProperty
    {
        string keyName;
        object value;
        string colName;
        public DeviceProperty(string keyName, object value, string colName)
        {
            this.keyName = keyName;
            this.value = value;
            this.colName = colName;
        }
        public DeviceProperty(string keyName, object value)
        {
            this.keyName = keyName;
            this.value = value;
            this.colName = keyName;
        }
    }
}