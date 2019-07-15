using System;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace udf
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
            // add in a more specific schema, we know what we want the types to be
            var partitions = spark
                            .Read()
                            .Option("header", true)
                            .Option("ignoreLeadingWhiteSpace", true)
                            .Option("ignoreTrailingWhiteSpace", true)                            
                            .Schema("name string, age int, city string") //instead of inferSchema we specify the schema
                            .Csv("./source.csv");
            
            partitions.PrintSchema();

            // create a UDF that we will use to put peoples ages into groups, in the udf's type spec (Udf<int, int>) the last value is the return type and everything before the types of any parameters
            // here we have one parameter (int) and return an int
            // I have named the parameter age
            var ageRange = Udf<int, int>((age) => {
                //Write whatever code you want here, it will be awesome.
                if(age < 30){
                    return 1;
                }

                if (age < 40){
                    return 2;
                }

                if( age < 67){
                    return 3;
                }

                return 4;
            });
            
            //This udf takes two values and returns an int (poor peter)
            var ageRangeUnlessPeter = Udf<int, string, int>((age, name) => {
                
                if(name == "peter"){
                    return 99;
                }

                if(age < 30){
                    return 1;
                }

                if (age < 40){
                    return 2;
                }

                if( age < 67){
                    return 3;
                }

                return 4;
            });

            //select all columns and append our new ageBucket UDF - use Alias to rename the column otherwise it is some weird name like "Int32 <Main>b__0_1(System.String)(age)" 
            var everything = partitions
                                .Select(
                                    partitions.Col("*"),                                                    //select all existing columns (could have said city or name etc)
                                    ageRange(partitions.Col("age")).Alias("ageBucket"),                     //call ageRange UDF for each row passing in "age" then alias the new column to ageBucket
                                    ageRangeUnlessPeter(partitions.Col("age"), partitions.Col("name"))      //call ageRangeUnlessPeter UDF passing in age and name
                                        .Alias("ageBucketUnlessPeter")                                      //    alias the new column to ageBucketUnlessPeter
                                );

            everything.Show();

            //write the data partitioned by the new ageBucket column
            everything.Write()
                            .Mode(SaveMode.Overwrite)
                            .Option("header", true)
                            .PartitionBy("ageBucket")
                            .Csv("./output-partitioned-age-bucket");

            //Now lets filter by the UDF!
            partitions.Filter(ageRangeUnlessPeter(partitions.Col("age"), partitions.Col("name")) == 99).Show();

        }
    }
}


