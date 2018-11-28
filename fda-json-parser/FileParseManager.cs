using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace fda_json_parser
{
    class FileParseManager
    {
        const string localFileDirectory = @"C:\Users\Joel\Desktop\fda_files";
        public async Task ParseUdiPartitionDataFiles()
        {
            ReadDataFiles();
        }

        void ReadDataFiles()
        {
            string[] dataFiles = Directory.GetFiles(localFileDirectory, "*.json");
            foreach(string dataFile in dataFiles)
            {
                using(StreamReader reader = new StreamReader(new FileStream(dataFile, FileMode.Open)))
                {
                    //Remove the meta data from the file before parsing
                    RemoveMetaDataObjectsFromFile(reader);
                    ReadJsonObjectsFromFile(reader);
                }
            }
        }

        void ReadJsonObjectsFromFile(StreamReader reader)
        {
        }

        void RemoveMetaDataObjectsFromFile(StreamReader reader)
        {
            //Throw out the first line which opens the entire file object
            reader.ReadLine();
            int openObjectCount = 0;
            int closedObjectCount = 0;
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
            } while (openObjectCount != closedObjectCount);
        }
    }
}