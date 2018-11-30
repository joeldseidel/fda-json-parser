using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Threading;
using MySql.Data.MySqlClient;
using System.Data;

namespace fda_json_parser
{
    class FileParseManager
    {
        const string localFileDirectory = @"C:\Users\Joel\Desktop\fda_files";

        private Queue queryQueue;
        private int totalFileCount = 0;
        private int readFileCount = 0;


        /// <summary>
        /// Manage the tasks of parsing the udi partitions
        /// </summary>
        public void ParseUdiPartitionDataFiles()
        {
            //Initialize the queue to be empty
            queryQueue = new Queue();
            //Get the data files that need to be parsed from the local directory
            string[] dataFiles = Directory.GetFiles(localFileDirectory, "*.json");
            totalFileCount = dataFiles.Length;
            //Create the threading task to read the queue as parsing threads add to it
            var queueReaderTask = new Task(() => QueueReader(), TaskCreationOptions.LongRunning);
            queueReaderTask.Start();
            //Begin adding the parsing methods to the threadpool and begin converting files to queries
            ReadDataFiles(dataFiles);
            //Create task to keep this thread running until the queue is empty
            Task waitTask = queueReaderTask.ContinueWith(t=>Console.WriteLine("Started waiting task"));
            waitTask.Wait();
        }
        /// <summary>
        /// Long running task to the consolidate the queries within the que into a batch query for the database
        /// </summary>
        void QueueReader()
        {
            //Create the connection string for the database connection
            MySqlConnectionStringBuilder connStringBuilder = new MySqlConnectionStringBuilder();
            // Connection string removed for security //
            using (MySqlConnection mConnection = new MySqlConnection(connStringBuilder.ToString()))
            {
                //Open the created connection to database
                mConnection.Open();
                int rowCounter = 0;
                string batchQuery = "";
                //Persistant loop while there are files in the queue or not all of the files have been read yet
                while (totalFileCount != readFileCount || queryQueue.Count > 0)
                {
                    //Dequeue next object if there are objects in the queue
                    if (queryQueue.Count > 0)
                    {
                        //Lock the queue to dequeue next query
                        lock (queryQueue)
                        {
                            //Add the query to the batched query
                            batchQuery += queryQueue.Dequeue().ToString();
                        }
                        //Add to the accumulating record counter
                        rowCounter += 1;
                    }
                    //When batch query is at 2500, committ to database
                    if (rowCounter >= 2500)
                    {
                        //Committ the 2500 queries to the database
                        Console.WriteLine("Starting database committ");
                        CommittBatchRowsToDatabase(mConnection, batchQuery);
                        Console.WriteLine("Completed database committ");
                        //Reset the query collection and row counter
                        rowCounter = 0;
                        batchQuery = "";
                    }
                }
            }
        }
        /// <summary>
        /// Committ a string of queries to the database
        /// </summary>
        /// <param name="mConnection">Connection to the MySql database</param>
        /// <param name="queries">The string of queries to be committed to the database</param>
        void CommittBatchRowsToDatabase(MySqlConnection mConnection, string queries)
        {
            //Create command for the collection of queries
            using (MySqlCommand writeDeviceCommand = new MySqlCommand(queries, mConnection))
            {
                //Execute the defined queries
                writeDeviceCommand.CommandType = CommandType.Text;
                writeDeviceCommand.ExecuteNonQuery();
            }
        }
        /// <summary>
        /// Add the parsing of data files method to the threadpool
        /// </summary>
        /// <param name="dataFiles">string array of the files to be parsed</param>
        void ReadDataFiles(string[] dataFiles)
        {
            //Loop through each of the files and add to the thread pool
            foreach (string dataFile in dataFiles)
            {
                //Add file parsing method of thie file to the threadpool queue
                ThreadPool.QueueUserWorkItem(new WaitCallback(ParseFile), dataFile);
            }
        }
        /// <summary>
        /// Method for parsing a file from text to a collection of queries being added to the queue
        /// </summary>
        /// <param name="dataFile">name of the data file to be parsed</param>
        void ParseFile(object dataFile)
        {
            Console.WriteLine("Started {0}", dataFile.ToString());
            //Open a stream reader to the file
            using (StreamReader reader = new StreamReader(new FileStream(dataFile.ToString(), FileMode.Open)))
            {
                //Remove the meta data from the file before parsing
                RemoveMetaDataObjectsFromFile(reader);
                //Read the json objects from the file and convert to queries
                ReadJsonObjectsFromFile(reader);
            }
            readFileCount += 1;
            Console.WriteLine("Completed {0}", dataFile.ToString());
        }
        /// <summary>
        /// Read the Json objects from the text of the data file
        /// </summary>
        /// <param name="reader">the stream reader opened to the file being parsed</param>
        void ReadJsonObjectsFromFile(StreamReader reader)
        {
            string readJsonObject = "";
            //Loop through the file until there are no more json objects to read
            do
            {
                ///Get the next complete json object
                readJsonObject = ReadNextJsonObject(reader);
                //Convert the json object which was just read to queries and add the query que
                ParseJsonObjectToQuery(readJsonObject);
            } while (readJsonObject != "");
        }
        /// <summary>
        /// Read a complete json object from the text of the data file
        /// </summary>
        /// <param name="reader">the stream reader opened to the file being parsed</param>
        /// <returns></returns>
        string ReadNextJsonObject(StreamReader reader)
        {
            //Counters for the open file
            //Amount of opening brackets - signifying the opening of a json object
            int openObjectCount = 0;
            //Amount of closing brackets - signifying the closing a json object
            int closedObjectCount = 0;
            string thisObjectString = "";
            //Read line from the file until an entire object is read
            do
            {
                //Read the next line from the file
                string thisLine = reader.ReadLine();
                //This line ends with a open bracket - this line is the beginning of a json object
                if (thisLine.EndsWith("{"))
                {
                    //Increment the number of json objects that are open
                    openObjectCount++;
                }
                //This line contains a closing bracket - this line is the end of a json object
                if (thisLine.Contains("}"))
                {
                    //Increment the number of json objects that are closed
                    closedObjectCount++;
                }
                //TODO this does not detect the EOF
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
        /// <summary>
        /// Remove the meta data json object from the top of the file before parsing the rest of it
        /// </summary>
        /// <param name="reader">the stream reader opened to the file being parsed</param>
        void RemoveMetaDataObjectsFromFile(StreamReader reader)
        {
            //Throw out the first line which opens the entire file object
            reader.ReadLine();
            ReadNextJsonObject(reader);
            //Throw out the next line, this is the beginning of the results array
            reader.ReadLine();
        }
        /// <summary>
        /// Convert the read json object to its respective queries
        /// </summary>
        /// <param name="argObj">json object string</param>
        void ParseJsonObjectToQuery(Object argObj)
        {
            string jsonObjectString = argObj.ToString();
            JObject readObject;
            try
            {
                //Parse the json string to jobject
                readObject = JObject.Parse(jsonObjectString);
                //Get the fda id which will be primary key for the async insert queries
                string fdaId = readObject["public_device_record_key"].ToString();
                //Create the queries for the child objects within the device record
                DoChildObjectQueries(readObject, fdaId);
                //Create the query for the main device properties
                DoDevicePropertiesQuery(readObject, fdaId);
            }
            //Catch the exceptions that are associated with corrupted records
            catch (JsonException) { }
            catch (NullReferenceException) { }
        }
        /// <summary>
        /// Get the main device properties for the device query
        /// </summary>
        /// <param name="readObject">json object to be parsed</param>
        /// <param name="fdaId">fda id of the object</param>
        void DoDevicePropertiesQuery(JObject readObject, string fdaId)
        {
            List<DeviceProperty> props = new List<DeviceProperty>();
            //Get each property if it exists and add to the property list
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
            //Get the subobject sterilization if it exists
            if (readObject.ContainsKey("sterilization"))
            {
                //Get the subobject from the main device object
                JObject sterilizationObject = (JObject)readObject["sterilization"];
                //Get the properties from the subobject and move the main device property list
                if (sterilizationObject.ContainsKey("is_sterile"))
                {
                    props.Add(new DeviceProperty("is_sterile", (bool)sterilizationObject.GetValue("is_sterile")));
                }
                if (sterilizationObject.ContainsKey("is_sterilization_prior_use"))
                {
                    props.Add(new DeviceProperty("is_sterilization_prior_use", (bool)sterilizationObject.GetValue("is_sterilization_prior_use")));
                }
                if (sterilizationObject.ContainsKey("sterilization_methods"))
                {
                    props.Add(new DeviceProperty("sterilization_methods", sterilizationObject.GetValue("sterilization_methods").ToString()));
                }
            }
            if (readObject.ContainsKey("version_or_model_number"))
            {
                props.Add(new DeviceProperty("version_or_model_number", readObject.GetValue("version_or_model_number").ToString()));
            }
            //Get the subobject product codes if it exists (it will)
            if (readObject.ContainsKey("product_codes"))
            {
                //Get the products code array
                JArray productCodesArray = (JArray)readObject["product_codes"];
                //If there is actually product codes for this device, get the openfda object properties
                if (productCodesArray.Count > 0)
                {
                    //Get the open fda sub sub object from the first element of the products code array
                    JObject openFda = (JObject)readObject["product_codes"][0]["openfda"];
                    //Get the properties of the sub sub object and add to the main device property list
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
            }
            #endregion
            //Create the query string for the main device
            string devicePropertiesQueryString = GetFdaDevicePropertyQuery("devices", fdaId, props);
            lock (queryQueue)
            {
                //Add the query string to the query queue
                queryQueue.Enqueue(devicePropertiesQueryString);
            }
        }
        /// <summary>
        /// Create the queries for the child objects of the main device
        /// </summary>
        /// <param name="readObject">the json device to parse</param>
        /// <param name="fdaId">fda id to identify the child objects</param>
        void DoChildObjectQueries(JObject readObject, string fdaId)
        {
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
        /// <summary>
        /// Create the query for the customer contacts subobject
        /// </summary>
        /// <param name="ccArr">array of customer contacts</param>
        /// <param name="fdaId">fda id key</param>
        void DoCustomerContactsQuery(JArray ccArr, string fdaId)
        {
            //Create a query for each of the customer contact records
            foreach(JObject cc in ccArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                //Get the properties of the customer contact object and add to property list
                if (cc.ContainsKey("phone"))
                {
                    props.Add(new DeviceProperty("phone", cc.GetValue("phone").ToString()));
                }
                if (cc.ContainsKey("email"))
                {
                    props.Add(new DeviceProperty("email", cc.GetValue("email").ToString()));
                }
                //Create the query for this customer contact record
                string ccQueryString = GetFdaDevicePropertyQuery("device_customer_contacts", fdaId, props);
                lock (queryQueue)
                {
                    //Add the customer contact query to the query queue
                    queryQueue.Enqueue(ccQueryString);
                }
            }
        }
        /// <summary>
        /// Create the query for the device sizes subobject
        /// </summary>
        /// <param name="dsArr">array of device sizes</param>
        /// <param name="fdaId">fda id key</param>
        void DoDeviceSizesQuery(JArray dsArr, string fdaId)
        {
            //Create a query for each of the device sizes records
            foreach(JObject ds in dsArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                //Get the properties of the device size object and add to the property list
                if (ds.ContainsKey("text"))
                {
                    props.Add(new DeviceProperty("text", ds.GetValue("text").ToString()));
                }
                if (ds.ContainsKey("type"))
                {
                    props.Add(new DeviceProperty("type", ds.GetValue("type").ToString()));
                }
                if (ds.ContainsKey("value"))
                {
                    props.Add(new DeviceProperty("value", ds.GetValue("value").ToString()));
                }
                if (ds.ContainsKey("unit"))
                {
                    props.Add(new DeviceProperty("unit", ds.GetValue("unit").ToString()));
                }
                //Create the query for this device size object record
                string dsQueryString = GetFdaDevicePropertyQuery("device_device_sizes", fdaId, props);
                lock (queryQueue)
                {
                    //Add the device size query to the query queue
                    queryQueue.Enqueue(dsQueryString);
                }
            }
        }
        /// <summary>
        /// Create the query for the gmdn terms subobject
        /// </summary>
        /// <param name="gtArr">array if gmdn terms</param>
        /// <param name="fdaId">fda id key</param>
        void DoGmdnTermsQuery(JArray gtArr, string fdaId)
        {
            //Create a query for each of the device gmdn terms
            foreach(JObject gt in gtArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                //Get the properties of the device gmdn terms object and add to the property list
                if (gt.ContainsKey("name"))
                {
                    props.Add(new DeviceProperty("name", gt.GetValue("name").ToString()));
                }
                if (gt.ContainsKey("definition"))
                {
                    props.Add(new DeviceProperty("definition", gt.GetValue("definition").ToString()));
                }
                //Create the query for this gmdn term record
                string gtQueryString = GetFdaDevicePropertyQuery("device_gmdn_terms", fdaId, props);
                lock (queryQueue)
                {
                    //Add the gmdn terms query to the query queue
                    queryQueue.Enqueue(gtQueryString);
                }
            }
        }
        /// <summary>
        /// Create the query for the device identifers subobject
        /// </summary>
        /// <param name="diArr">array of device identifiers</param>
        /// <param name="fdaId">fda id key</param>
        void DoDeviceIdentifiersQuery(JArray diArr, string fdaId)
        {
            //Create a query for each of the device identifiers
            foreach(JObject di in diArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                //Get the properties of the device identifiers and add to the property list
                if (di.ContainsKey("id"))
                {
                    props.Add(new DeviceProperty("id", di.GetValue("id").ToString()));
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
                    props.Add(new DeviceProperty("type", di.GetValue("type").ToString()));
                }
                if (di.ContainsKey("unit_of_use_id"))
                {
                    props.Add(new DeviceProperty("unit_of_use_id", di.GetValue("unit_of_use_id").ToString()));
                }
                //Create the query for this device identifiers record
                string diQueryString = GetFdaDevicePropertyQuery("device_identifiers", fdaId, props);
                lock (queryQueue)
                {
                    //add the device identifiers query to the query queue
                    queryQueue.Enqueue(diQueryString);
                }
            }
        }
        /// <summary>
        /// Create the query for the premarket submission subobject
        /// </summary>
        /// <param name="psArr">array of the premarket submission objects</param>
        /// <param name="fdaId">fda id key</param>
        void DoPremarketSubmissionQuery(JArray psArr, string fdaId)
        {
            //Create a query for each of the premarket submissions
            foreach(JObject ps in psArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                //Get the properties from the premarket submission and add to the properties list
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
                //Create the query string for this premarket submission record
                string psQueryString = GetFdaDevicePropertyQuery("device_premarket_submissions", fdaId, props);
                lock (queryQueue)
                {
                    //Add the premarket submission query to the query queue
                    queryQueue.Enqueue(psQueryString);
                }
            }
        }
        /// <summary>
        /// Create the query for the device product codes
        /// </summary>
        /// <param name="pcArr">array of product codes</param>
        /// <param name="fdaId">fda id key</param>
        void DoDeviceProductCodesQuery(JArray pcArr, string fdaId)
        {
            //Create a query for each of the product codes
            foreach(JObject pc in pcArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                //Get the properties from the device product codes and add to the properties list
                if (pc.ContainsKey("code"))
                {
                    props.Add(new DeviceProperty("code", pc.GetValue("code").ToString()));
                }
                if (pc.ContainsKey("name"))
                {
                    props.Add(new DeviceProperty("name", pc.GetValue("name").ToString()));
                }
                //Create the query string for this product codes record
                string pcQueryString = GetFdaDevicePropertyQuery("device_product_codes", fdaId, props);
                lock (queryQueue)
                {
                    //Add the product codes query to the query queue
                    queryQueue.Enqueue(pcQueryString);
                }
            }
        }
        /// <summary>
        /// Create the query for the device storage record
        /// </summary>
        /// <param name="dsArr">array of the storage records</param>
        /// <param name="fdaId">fda id key</param>
        void DoDeviceStorageQuery(JArray dsArr, string fdaId)
        {
            //Create a query for each of the storage records
            foreach(JObject ds in dsArr)
            {
                List<DeviceProperty> props = new List<DeviceProperty>();
                //Get the properties from the storage records and add to the properties list
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
                    props.Add(new DeviceProperty("type", ds.GetValue("type").ToString()));
                }
                //Create the query for the storage records
                string dsQueryString = GetFdaDevicePropertyQuery("device_storage", fdaId, props);
                lock (queryQueue)
                {
                    //Add the storage record query to the query queue
                    queryQueue.Enqueue(dsQueryString);
                }
            }
        }
        /// <summary>
        /// Create a query for a specific device property set
        /// </summary>
        /// <param name="tableName">name of the subobject this query comes from</param>
        /// <param name="fdaId">fda id key</param>
        /// <param name="props">list of property values and names to add to the query</param>
        /// <returns></returns>
        string GetFdaDevicePropertyQuery(string tableName, string fdaId, List<DeviceProperty> props)
        {
            //Create the syntax for the query
            string writeDeviceColumnsSql = "INSERT INTO " + tableName + "(fda_id";
            string writeDeviceValuesSql = ") VALUES ('" + fdaId + "'";
            //For each property provided, add to the query string
            foreach(DeviceProperty thisProperty in props)
            {
                //Add the column name to the query string
                writeDeviceColumnsSql += ", " + thisProperty.GetColumnName();
                writeDeviceValuesSql += ", ";
                //Add the value of the property to the query string
                object propertyValue = thisProperty.GetValue();
                //Add the ' character to surround the string if the column value is a string
                if (propertyValue.GetType() == typeof(string))
                {
                    string propValString = propertyValue.ToString();
                    propValString = propValString.Replace("'", "''");
                    writeDeviceValuesSql += "'" + propValString + "'";
                }
                //Convert the boolean data type to 1 or 0 if the column value is a boolean
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
                //No conversion necessary if the column value is an integer
                else if (propertyValue.GetType() == typeof(int))
                {
                    writeDeviceValuesSql += propertyValue;
                }
            }
            //Combine the columns and values to create the entire query
            return writeDeviceColumnsSql + writeDeviceValuesSql + ");";
        }
    }
}