using Microsoft.Spark.Sql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace chunk_file
{
    class Program
    {
        static void Main(string[] args)
        {
            Debugger.Launch();
            Console.WriteLine("Hello Spark!");

            var spark = SparkSession
                .Builder()
                .GetOrCreate();

            spark.Udf().Register<string, List<string[]>>("ParseTextFile", (text) => Split(text));

            var file = spark.Read().Option("wholeFile", true).Option("lineSep", "\0").Text(@"C:\git\files\newline-as-data.txt");
            file.Show();

            var exploded = file.Select(Functions.Explode(
               Functions.CallUDF("ParseTextFile", file.Col("value"))
           ));

           exploded.Show();
           var allColumns = exploded
                               .Select(exploded.Col("col").GetItem(0), 
                                       Functions.ToDate(exploded.Col("col").GetItem(1), "yyyyMMdd"),
                                        exploded.Col("col").GetItem(2),
                                        exploded.Col("col").GetItem(3),
                                        exploded.Col("col").GetItem(4),
                                        exploded.Col("col").GetItem(5),
                                        exploded.Col("col").GetItem(6),
                                        exploded.Col("col").GetItem(7).Cast("float"),
                                        exploded.Col("col").GetItem(8),
                                        exploded.Col("col").GetItem(9),
                                        exploded.Col("col").GetItem(10).Cast("int")
                                        );


            var final = allColumns.ToDF("RecType", "Date", "Productnumber", "TAG", "Contract", "Filler1", "Code", "Version", "newline", "FILENAME", "Recnumber");
            final.PrintSchema();
            final.Show();
            
        }

        private static List<string[]> Split(string textFile)
        {
            const int chunkSize = 84;
            int fileLength = textFile.Length;
            var list = new List<string[]>();
            for (var offset = 0; offset < fileLength; offset += chunkSize)
            {
                var row = textFile.Substring(offset, chunkSize);
                list.Add(
                    new string[]
                    {
                        row.Substring(0, 1), row.Substring(1, 8), row.Substring(9, 15), row.Substring(24, 11), row.Substring(35, 11), row.Substring(46, 1), row.Substring(47, 3), row.Substring(50, 3),
                        row.Substring(54, 1), row.Substring(55, 25), row.Substring(80, 4)
                    }
                );
            }

            return list;
        }
    }
}
