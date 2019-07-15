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

            //Read the partitioned csv's back in
            // add in a more specific schema, we know what we want the types to be
            // note - output-partitioned-city-csv is the parent directory with sub-folders for each city and they will
            // all be found an read in, awesome!
            var partitions = spark
                            .Read()
                            .Option("header", true)
                            .Option("ignoreLeadingWhiteSpace", true)
                            .Option("ignoreTrailingWhiteSpace", true)                            
                            .Schema("name string, age int, city string") //instead of inferSchema we specify the schema
                            .Csv("./output-partitioned-city-csv");
            
            partitions.PrintSchema();

            //Write the partitiond csv's back out as a single partition

            partitions.Write().Mode(SaveMode.Overwrite).Option("header", true).Csv("./output-single-partition-again");
        }
    }
}
