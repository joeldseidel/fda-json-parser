using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;

namespace fda_json_parser
{
    class FileParseManager
    {
        const string localFileDirectory = @"C:\Users\Joel\Desktop\fda_files";

        private Queue queryQueue;

        public async Task ParseUdiPartitionDataFiles()
        {
            queryQueue = new Queue();
            ReadDataFiles();
        }

        void ReadDataFiles()
        {
            string[] dataFiles = Directory.GetFiles(localFileDirectory, "*.json");
            foreach (string dataFile in dataFiles)
            {
                using (StreamReader reader = new StreamReader(new FileStream(dataFile, FileMode.Open)))
                {
                    //Remove the meta data from the file before parsing
                    RemoveMetaDataObjectsFromFile(reader);
                    ReadJsonObjectsFromFile(reader);
                }
            }
        }

        void ReadJsonObjectsFromFile(StreamReader reader)
        {
            string readJsonObject = "";
            do
            {
                readJsonObject = ReadNextJsonObject(reader);
                ParseJsonObjectToQuery(readJsonObject);
            } while (readJsonObject != "");
        }

        string ReadNextJsonObject(StreamReader reader)
        {
            int openObjectCount = 0;
            int closedObjectCount = 0;
            string thisObjectString = "";
            do
            {
                string thisLine = reader.ReadLine();
                if (thisLine.Contains("{"))
                {
                    openObjectCount++;
                }
                if (thisLine.Contains("}"))
                {
                    closedObjectCount++;
                }
                if (closedObjectCount > openObjectCount)
                {
                    //This is EOF
                    return "";
                }
                thisObjectString += thisLine;
            } while (openObjectCount != closedObjectCount);
            //Trim the comma off of the end
            thisObjectString = thisObjectString.Trim(',');
            return thisObjectString;
        }
        void RemoveMetaDataObjectsFromFile(StreamReader reader)
        {
            //Throw out the first line which opens the entire file object
            reader.ReadLine();
            ReadNextJsonObject(reader);
            //Throw out the next line, this is the beginning of the results array
            reader.ReadLine();
        }
        void ParseJsonObjectToQuery(Object argObj)
        {
            Console.WriteLine("Start parse");
            string jsonObjectString = argObj.ToString();
            JObject readObject = JObject.Parse(jsonObjectString);
            //Get the fda id which will be primary key for the async insert queries
            string fdaId = readObject["public_device_record_key"].ToString();

            DoChildObjectQueries(readObject, fdaId);
            DoDevicePropertiesQuery(readObject, fdaId);
            Console.WriteLine("Done");
            Console.ReadKey();
        }
        void DoDevicePropertiesQuery(JObject readObject, string fdaId)
        {
            List<DeviceProperty> props = new List<DeviceProperty>();
            #region
            if (readObject.ContainsKey("brand_name"))
            {
                props.Add(new DeviceProperty("brand_name", readObject.GetValue("brand_name").ToString()));
            }
            if (readObject.ContainsKey("catalog_number"))
            {
                props.Add(new DeviceProperty("catalog_number", readObject.GetValue("catalog_number").ToString()));
            }
            if (readObject.ContainsKey("commercial_distribution_end_date"))
            {
                props.Add(new DeviceProperty("commercial_distribution_end_date", readObject.GetValue("commercial_distribution_end_date").ToString()));
            }
            if (readObject.ContainsKey("commercial_distribution_status"))
            {
                props.Add(new DeviceProperty("commercial_distribution_status", readObject.GetValue("commercial_distribution_status").ToString()));
            }
            if (readObject.ContainsKey("company_name"))
            {
                props.Add(new DeviceProperty("company_name", readObject.GetValue("company_name").ToString()));
            }
            if (readObject.ContainsKey("device_count_in_base_package"))
            {
                props.Add(new DeviceProperty("device_count_in_base_package", (int)readObject.GetValue("device_count_in_base_package")));
            }
            if (readObject.ContainsKey("device_description"))
            {
                props.Add(new DeviceProperty("device_description", readObject.GetValue("device_description").ToString()));
            }
            if (readObject.ContainsKey("has_donation_id_number"))
            {
                props.Add(new DeviceProperty("has_donation_id_number", (bool)readObject.GetValue("has_donation_id_number")));
            }
            if (readObject.ContainsKey("has_expiration_date"))
            {
                props.Add(new DeviceProperty("has_expiration_date", (bool)readObject.GetValue("has_expiration_date")));
            }
            if (readObject.ContainsKey("has_lot_or_batch_number"))
            {
                props.Add(new DeviceProperty("has_lot_or_batch_number", (bool)readObject.GetValue("has_lot_or_batch_number")));
            }
            if (readObject.ContainsKey("has_manufacturing_date"))
            {
                props.Add(new DeviceProperty("has_manufacturing_date", (bool)readObject.GetValue("has_manufacturing_date")));
            }
            if (readObject.ContainsKey("has_serial_number"))
            {
                props.Add(new DeviceProperty("has_serial_number", (bool)readObject.GetValue("has_serial_number")));
            }
            if (readObject.ContainsKey("is_combination_product"))
            {
                props.Add(new DeviceProperty("is_combination_product", (bool)readObject.GetValue("is_combination_product")));
            }
            if (readObject.ContainsKey("is_direct_marking_exempt"))
            {
                props.Add(new DeviceProperty("is_direct_marking_exempt", (bool)readObject.GetValue("is_direct_marking_exempt")));
            }
            if (readObject.ContainsKey("is_hct_p"))
            {
                props.Add(new DeviceProperty("is_hct_p", (bool)readObject.GetValue("is_hct_p")));
            }
            if (readObject.ContainsKey("is_kit"))
            {
                props.Add(new DeviceProperty("is_kit", (bool)readObject.GetValue("is_kit")));
            }
            if (readObject.ContainsKey("is_labeled_as_no_nrl"))
            {
                props.Add(new DeviceProperty("is_labeled_as_no_nrl", (bool)readObject.GetValue("is_labeled_as_no_nrl")));
            }
            if (readObject.ContainsKey("is_labeled_as_nrl"))
            {
                props.Add(new DeviceProperty("is_labeled_as_nrl", (bool)readObject.GetValue("is_labeled_as_nrl")));
            }
            if (readObject.ContainsKey("is_otc"))
            {
                props.Add(new DeviceProperty("is_otc", (bool)readObject.GetValue("is_otc")));
            }
            if (readObject.ContainsKey("is_pm_exempt"))
            {
                props.Add(new DeviceProperty("is_pm_exempt", (bool)readObject.GetValue("is_pm_exempt")));
            }
            if (readObject.ContainsKey("is_rx"))
            {
                props.Add(new DeviceProperty("is_rx", (bool)readObject.GetValue("is_rx")));
            }
            if (readObject.ContainsKey("is_single_use"))
            {
                props.Add(new DeviceProperty("is_single_use", (bool)readObject.GetValue("is_single_use")));
            }
            if (readObject.ContainsKey("labeler_duns_number"))
            {
                props.Add(new DeviceProperty("labeler_duns_number", readObject.GetValue("labeler_duns_number").ToString()));
            }
            if (readObject.ContainsKey("mri_safety"))
            {
                props.Add(new DeviceProperty("mri_safety", readObject.GetValue("mri_safety").ToString()));
            }
            if (readObject.ContainsKey("public_version_date"))
            {
                props.Add(new DeviceProperty("public_version_date", readObject.GetValue("public_version_date").ToString()));
            }
            if (readObject.ContainsKey("public_version_number"))
            {
                props.Add(new DeviceProperty("public_version_number", readObject.GetValue("public_version_number").ToString()));
            }
            if (readObject.ContainsKey("public_version_status"))
            {
                props.Add(new DeviceProperty("public_version_status", readObject.GetValue("public_version_status").ToString()));
            }
            if (readObject.ContainsKey("publish_date"))
            {
                props.Add(new DeviceProperty("publish_date", readObject.GetValue("publish_date").ToString()));
            }
            if (readObject.ContainsKey("record_key"))
            {
                props.Add(new DeviceProperty("record_key", readObject.GetValue("record_key").ToString()));
            }
            if (readObject.ContainsKey("record_status"))
            {
                props.Add(new DeviceProperty("record_status", readObject.GetValue("record_status").ToString()));
            }
            if (readObject.ContainsKey("is_sterile"))
            {
                props.Add(new DeviceProperty("is_sterile", (bool)readObject.GetValue("is_sterile")));
            }
            if (readObject.ContainsKey("is_sterilization_prior_use"))
            {
                props.Add(new DeviceProperty("is_sterilization_prior_use", (bool)readObject.GetValue("is_sterilization_prior_use")));
            }
            if (readObject.ContainsKey("sterilization_methods"))
            {
                props.Add(new DeviceProperty("sterilization_methods", readObject.GetValue("sterilization_methods").ToString()));
            }
            if (readObject.ContainsKey("version_or_model_number"))
            {
                props.Add(new DeviceProperty("version_or_model_number", readObject.GetValue("version_or_model_number").ToString()));
            }
            if (readObject.ContainsKey("product_codes"))
            {
                JObject openFda = (JObject)readObject["product_codes"][0];
                if (openFda.ContainsKey("device_class"))
                {
                    props.Add(new DeviceProperty("device_class", openFda.GetValue("device_class").ToString()));
                }
                if (openFda.ContainsKey("device_name"))
                {
                    props.Add(new DeviceProperty("device_name", openFda.GetValue("device_name").ToString()));
                }
                if (openFda.ContainsKey("fei_number"))
                {
                    props.Add(new DeviceProperty("fei_number", openFda.GetValue("fei_number").ToString()));
                }
                if (openFda.ContainsKey("medical_specialty_description"))
                {
                    props.Add(new DeviceProperty("medical_specialty_description", openFda.GetValue("medical_specialty_description").ToString()));
                }
                if (openFda.ContainsKey("regulation_number"))
                {
                    props.Add(new DeviceProperty("regulation_number", openFda.GetValue("regulation_number").ToString()));
                }
            }
            #endregion
            string devicePropertiesQueryString = GetFdaDevicePropertyQuery("device", fdaId, props);
            queryQueue.Enqueue(devicePropertiesQueryString);
        }
        void DoChildObjectQueries(JObject readObject, string fdaId)
        {
            //Create the queries for the subobjects
            if (readObject.ContainsKey("customer_contacts"))
            {
                DoCustomerContactsQuery((JArray)readObject["customer_contacts"], fdaId);
            }
            if (readObject.ContainsKey("device_sizes"))
            {
                DoDeviceSizesQuery((JArray)readObject["device_sizes"], fdaId);
            }
            if (readObject.ContainsKey("gmdn_terms"))
            {
                DoGmdnTermsQuery((JArray)readObject["gmdn_terms"], fdaId);
            }
            if (readObject.ContainsKey("identifiers"))
            {
                DoDeviceIdentifiersQuery((JArray)readObject["identifiers"], fdaId);
            }
            if (readObject.ContainsKey("premarket_submissions"))
            {
                DoPremarketSubmissionQuery((JArray)readObject["premarket_submissions"], fdaId);
            }
            if (readObject.ContainsKey("product_codes"))
            {
                DoDeviceProductCodesQuery((JArray)readObject["product_codes"], fdaId);
            }
            if (readObject.ContainsKey("storage"))
            {
                DoDeviceStorageQuery((JArray)readObject["storage"], fdaId);
            }
        }
        void DoCustomerContactsQuery(JArray ccArr, string fdaId)
        {
            foreach(JObject cc in ccArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                if (cc.ContainsKey("phone"))
                {
                    props.Add(new DeviceProperty("phone", cc.GetValue("phone").ToString()));
                }
                if (cc.ContainsKey("email"))
                {
                    props.Add(new DeviceProperty("email", cc.GetValue("email").ToString()));
                }
                string ccQueryString = GetFdaDevicePropertyQuery("device_customer_contacts", fdaId, props);
                queryQueue.Enqueue(ccQueryString);
            }
        }
        void DoDeviceSizesQuery(JArray dsArr, string fdaId)
        {
            foreach(JObject ds in dsArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                if (ds.ContainsKey("text"))
                {
                    props.Add(new DeviceProperty("text", ds.GetValue("text").ToString(), "size_text"));
                }
                if (ds.ContainsKey("type"))
                {
                    props.Add(new DeviceProperty("type", ds.GetValue("type").ToString(), "size_type"));
                }
                if (ds.ContainsKey("value"))
                {
                    props.Add(new DeviceProperty("value", ds.GetValue("value").ToString(), "size_value"));
                }
                if (ds.ContainsKey("unit"))
                {
                    props.Add(new DeviceProperty("unit", ds.GetValue("unit").ToString()));
                }
                string dsQueryString = GetFdaDevicePropertyQuery("device_device_sizes", fdaId, props);
                queryQueue.Enqueue(dsQueryString);
            }
        }
        void DoGmdnTermsQuery(JArray gtArr, string fdaId)
        {
            foreach(JObject gt in gtArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                if (gt.ContainsKey("name"))
                {
                    props.Add(new DeviceProperty("name", gt.GetValue("name").ToString()));
                }
                if (gt.ContainsKey("definition"))
                {
                    props.Add(new DeviceProperty("definition", gt.GetValue("definition").ToString()));
                }
                string gtQueryString = GetFdaDevicePropertyQuery("device_gmdn_terms", fdaId, props);
                queryQueue.Enqueue(gtQueryString);
            }
        }
        void DoDeviceIdentifiersQuery(JArray diArr, string fdaId)
        {
            foreach(JObject di in diArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                if (di.ContainsKey("id"))
                {
                    props.Add(new DeviceProperty("id", di.GetValue("id").ToString(), "identifier_id"));
                }
                if (di.ContainsKey("issuing_agency"))
                {
                    props.Add(new DeviceProperty("issuing_agency", di.GetValue("issuing_agency").ToString()));
                }
                if (di.ContainsKey("package_discontinue_date"))
                {
                    props.Add(new DeviceProperty("package_discontinue_date", di.GetValue("package_discontinue_date").ToString()));
                }
                if (di.ContainsKey("package_status"))
                {
                    props.Add(new DeviceProperty("package_status", di.GetValue("package_status").ToString()));
                }
                if (di.ContainsKey("package_type"))
                {
                    props.Add(new DeviceProperty("package_type", di.GetValue("package_type").ToString()));
                }
                if (di.ContainsKey("quantity_per_package"))
                {
                    props.Add(new DeviceProperty("quantity_per_package", di.GetValue("quantity_per_package").ToString()));
                }
                if (di.ContainsKey("type"))
                {
                    props.Add(new DeviceProperty("type", di.GetValue("type").ToString(), "identifier_type"));
                }
                if (di.ContainsKey("unit_of_use_id"))
                {
                    props.Add(new DeviceProperty("unit_of_use_id", di.GetValue("unit_of_use_id").ToString()));
                }
                string diQueryString = GetFdaDevicePropertyQuery("device_identifiers", fdaId, props);
                queryQueue.Enqueue(diQueryString);
            }
        }
        void DoPremarketSubmissionQuery(JArray psArr, string fdaId)
        {
            foreach(JObject ps in psArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                if (ps.ContainsKey("submission_number"))
                {
                    props.Add(new DeviceProperty("submission_number", ps.GetValue("submission_number").ToString()));
                }
                if (ps.ContainsKey("supplement_number"))
                {
                    props.Add(new DeviceProperty("supplement_number", ps.GetValue("supplement_number").ToString()));
                }
                if (ps.ContainsKey("submission_type"))
                {
                    props.Add(new DeviceProperty("submission_type", ps.GetValue("submission_type").ToString()));
                }
                string psQueryString = GetFdaDevicePropertyQuery("device_premarket_submissions", fdaId, props);
                queryQueue.Enqueue(psQueryString);
            }
        }
        void DoDeviceProductCodesQuery(JArray pcArr, string fdaId)
        {
            foreach(JObject pc in pcArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                if (pc.ContainsKey("code"))
                {
                    props.Add(new DeviceProperty("code", pc.GetValue("code").ToString()));
                }
                if (pc.ContainsKey("name"))
                {
                    props.Add(new DeviceProperty("name", pc.GetValue("name").ToString()));
                }
                string pcQueryString = GetFdaDevicePropertyQuery("device_product_codes", fdaId, props);
                queryQueue.Enqueue(pcQueryString);
            }
        }
        void DoDeviceStorageQuery(JArray dsArr, string fdaId)
        {
            foreach(JObject ds in dsArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                if (ds.ContainsKey("high"))
                {
                    JObject highSubObj = (JObject)ds["high"];
                    if (highSubObj.ContainsKey("value"))
                    {
                        props.Add(new DeviceProperty("value", highSubObj.GetValue("value").ToString(), "high_value"));
                    }
                    if (highSubObj.ContainsKey("unit"))
                    {
                        props.Add(new DeviceProperty("unit", highSubObj.GetValue("unit").ToString(), "high_unit"));
                    }
                }
                if (ds.ContainsKey("low"))
                {
                    JObject lowSubObj = (JObject)ds["low"];
                    if (lowSubObj.ContainsKey("value")) {
                        props.Add(new DeviceProperty("value", lowSubObj.GetValue("value").ToString(), "low_value"));
                    }
                    if (lowSubObj.ContainsKey("unit"))
                    {
                        props.Add(new DeviceProperty("unit", lowSubObj.GetValue("unit").ToString(), "low_unit"));
                    }
                }
                if (ds.ContainsKey("special_conditions"))
                {
                    props.Add(new DeviceProperty("special_conditions", ds.GetValue("special_conditions").ToString()));
                }
                if (ds.ContainsKey("type"))
                {
                    props.Add(new DeviceProperty("type", ds.GetValue("type").ToString(), "storage_type"));
                }
                string dsQueryString = GetFdaDevicePropertyQuery("device_storage", fdaId, props);
                queryQueue.Enqueue(dsQueryString);
            }
        }
        string GetFdaDevicePropertyQuery(string tableName, string fdaId, List<DeviceProperty> props)
        {
            string writeDeviceColumnsSql = "INSERT INTO " + tableName + "(fda_id";
            string writeDeviceValuesSql = ") VALUES ('" + fdaId + "'";
            foreach(DeviceProperty thisProperty in props)
            {
                writeDeviceColumnsSql += ", " + thisProperty.GetColumnName();
                writeDeviceValuesSql += ", ";
                object propertyValue = thisProperty.GetValue();
                if (propertyValue.GetType() == typeof(string))
                {
                    string propValString = propertyValue.ToString();
                    propValString = propValString.Replace("'", "''");
                    writeDeviceValuesSql += "'" + propValString + "'";
                }
                else if (propertyValue.GetType() == typeof(bool))
                {
                    if ((bool)propertyValue)
                    {
                        writeDeviceValuesSql += "1";
                    }
                    else
                    {
                        writeDeviceValuesSql += "0";
                    }
                }
                else if (propertyValue.GetType() == typeof(int))
                {
                    writeDeviceValuesSql += propertyValue;
                }
            }
            return writeDeviceColumnsSql + writeDeviceValuesSql + ")";
        }
    }
}