using System;
using System.Threading.Tasks;

namespace fda_json_parser
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //Display the opening text on the console
            Console.WriteLine("Joel Seidel, Maverick Systems LLC");
            Console.WriteLine("Let's parse the FDA's files, shall we?");
            Console.Write("\nPress any key to continue");
            Console.ReadKey();

            //Fetch the fda files from the fda servers and save to local machine
            FDAFileManager fdaFileManager = new FDAFileManager();
            await fdaFileManager.FetchFdaDataFiles();

            //Parse the fda files and committ parsed data to the database
            FileParseManager fileParseManager = new FileParseManager();
            fileParseManager.ParseUdiPartitionDataFiles();
            Console.ReadKey();
        }
    }
}
