using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.IO.Compression;
using Newtonsoft.Json.Linq;

namespace fda_json_parser
{
    class FDAFileManager
    {
        const string fdaUrl = "https://api.fda.gov/download.json";
        const string availableFilesFilePath = @"C:\Users\Joel\Desktop\fda_files\available_files.json";
        const string localFileDirectory = @"C:\Users\Joel\Desktop\fda_files";

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task FetchFdaDataFiles()
        {
            await GetAvailableFilesFile();
            List<string> availableUdiPartFileUrlList = GetAvailableFileUrls();
            await DownloadFdaDataFiles(availableUdiPartFileUrlList);
            DecompressUdiPartitionDataFile();
            Console.ReadKey();
        }

        /// <summary>
        /// Download the file from the fda which shows the files available for downloading
        /// </summary>
        /// <returns></returns>
        async Task GetAvailableFilesFile()
        {
            //Download the available files file from the fda service
            await DownloadFileFromUrl(fdaUrl, availableFilesFilePath);
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

        /// <summary>
        /// Download the zipped udi partition file from the fda service
        /// </summary>
        /// <param name="udiPartitionFileUrlList">List of available udi partition files urls</param>
        /// <returns></returns>
        async Task DownloadFdaDataFiles(List<string> udiPartitionFileUrlList)
        {
            //Loop through each udi partition url string in the available udi partition file list and download by url
            foreach(string udiPartUrl in udiPartitionFileUrlList)
            {
                //Get the file name from the url in the available udi partition file list
                string udiPartFileName = Path.GetFileName(new Uri(udiPartUrl).LocalPath);
                //Download the file from the specified url to the local file with the determined name
                await DownloadFileFromUrl(udiPartUrl, Path.Combine(localFileDirectory, udiPartFileName));
            }
        }

        /// <summary>
        /// Unzip the downloaded fda data files
        /// </summary>
        void DecompressUdiPartitionDataFile()
        {
            //Get the zip files within the local directory
            string[] zippedUdiPartFiles = Directory.GetFiles(localFileDirectory, "*.zip");
            //For each udi partition zipped file path, unzip the file into a file of the same name with a json type
            foreach(string udiPartFilePath in zippedUdiPartFiles)
            {
                //Open the zip archive of the zipped data file
                using (ZipArchive archive = ZipFile.OpenRead(udiPartFilePath))
                {
                    //For each zip entry, decompress into a file
                    foreach(ZipArchiveEntry entry in archive.Entries)
                    {
                        //Get the name of the file without the .zip extension
                        string localDecompressedLocationFile = udiPartFilePath.Replace(".zip", "");
                        //Extract the zipped json data file into a json file of the same name
                        entry.ExtractToFile(localDecompressedLocationFile);
                    }
                }
            }
        }

        private async Task DownloadFileFromUrl(string url, string localFile)
        {
            //Create HttpClient for downloading the file stream
            using (HttpClient httpClient = new HttpClient())
            {
                //Create the stream for reading data from the specified location
                using (HttpResponseMessage response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead))
                using (Stream readingStream = await response.Content.ReadAsStreamAsync())
                {
                    //Read the stream of the located file into the specified local file
                    using (Stream writingStream = File.Open(localFile, FileMode.Create))
                    {
                        //Await the completion of reading the stream
                        await readingStream.CopyToAsync(writingStream);
                    }
                }
            }
        }
    }
}