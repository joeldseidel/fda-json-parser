using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace fda_json_parser
{
    class FDAFileManager
    {
        const string fdaUrl = "https://api.fda.gov/download.json";
        const string availableFilesFilePath = @"C:\Users\Joel\Desktop\fda_files\available_files.json";

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task FetchFdaDataFiles()
        {
            await GetAvailableFilesFile();
            GetAvailableFileUrls();
            Console.ReadKey();
        }

        /// <summary>
        /// Download the file from the fda which shows the files available for downloading
        /// </summary>
        /// <returns></returns>
        async Task GetAvailableFilesFile()
        {
            //Create HttpClient for downloading the file stream
            using (HttpClient httpClient = new HttpClient())
            {
                //Create the stream for reading data from the specified location
                using (HttpResponseMessage response = await httpClient.GetAsync(fdaUrl, HttpCompletionOption.ResponseHeadersRead))
                using (Stream readingStream = await response.Content.ReadAsStreamAsync())
                {
                    //Read the stream of the located file into the specified local file
                    using(Stream writingStream = File.Open(availableFilesFilePath, FileMode.Create))
                    {
                        //Await the completion of reading the stream
                        await readingStream.CopyToAsync(writingStream);
                    }
                }
            }
        }

        /// <summary>
        /// Read and parse the downloaded json object within the available fda files file for a list of the urls of the udi partition files
        /// </summary>
        /// <returns>List of strings of the urls of the udi partition files</returns>
        List<string> GetAvailableFileUrls()
        {
            //Read the available files file from the location it was just downloaded to
            string availableFileJsonString = File.ReadAllText(availableFilesFilePath);
            //Convert the entire file into a json object
            JObject fdaFileObject = JObject.Parse(availableFileJsonString);
            //Get the udi child object of the main file
            JObject udiFileObject = (JObject)fdaFileObject["results"]["device"]["udi"];

            //Display discovered record count and date the available files file was exported
            Console.WriteLine(String.Format("Found {0} records to parse. Exported on {1} \n", udiFileObject.GetValue("total_records").ToString(), udiFileObject.GetValue("export_date").ToString()));

            //Get the json array of the udi partitions
            JArray udiFilePartitions = (JArray)udiFileObject["partitions"];
            List<string> udiPartitionFileList = new List<string>();
            //Loop through each json object within the json array of udi partitions
            foreach(JObject udiPartition in udiFilePartitions)
            {
                //Get data from the current udi partition record
                string partitionFileUrl = udiPartition.GetValue("file").ToString();
                string partitionFileSize = udiPartition.GetValue("size_mb").ToString();
                string partitionDisplayName = udiPartition.GetValue("display_name").ToString();
                //Add the listed url to the list of udi partition file urls
                udiPartitionFileList.Add(partitionFileUrl);

                //Display the display name and file size of the current udi partition
                Console.WriteLine("FILE:  {1}    SIZE:  {0}" , partitionFileSize, partitionDisplayName);
            }
            return udiPartitionFileList;
        }
    }
}