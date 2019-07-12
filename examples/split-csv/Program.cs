using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace split_csv
{
    class Program
    {        
        static void Main(string[] args)
        {
            Console.WriteLine("Hello Spark!");

            var spark = SparkSession
                .Builder()
                .GetOrCreate();

            //Read a single CSV file
            var source = spark
                            .Read()
                            .Option("header", true)
                            .Option("inferShchema", true)
                            .Option("ignoreLeadingWhiteSpace", true)
                            .Option("ignoreTrailingWhiteSpace", true)
                            .Csv("./source.csv");

            //Write that CSV into many different CSV files, partitioned by city
            source.Write()
                    .Mode(SaveMode.Overwrite)
                    .Option("header", true)
                    .PartitionBy("city")
                    .Csv("./output-partitioned-city-csv");

        }
    }
}
