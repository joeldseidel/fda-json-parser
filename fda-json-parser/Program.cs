using System;

namespace fda_json_parser
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Joel Seidel, Maverick Systems LLC");
            Console.WriteLine("Let's parse the FDA's files, shall we?");
            Console.Write("\nPress any key to continue");
            Console.ReadKey();

            FDAFileManager fdaFileManager = new FDAFileManager();
        }
    }
}
