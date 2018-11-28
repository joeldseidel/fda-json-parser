using System.IO;
using System.Net.Http;
using System.Threading.Tasks;

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
    }
}