package com.dev.bim.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkUDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("SparkUDF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
 //val data = "file:///D:\\bigdata\\datasets\\bank-full.csv"
 val data = "file:///D:\\bigdata\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("delimiter",",").load(data)
    df.createOrReplaceTempView("tab")

    //dSL queries -- Domain Specific language
  //  val res = df.where($"balance">7000 && $"marital"==="married")
    // SQL language
    //val res = spark.sql("select * from tab where balance>7000 && marital = 'married')
    //val res = spark.sql("select *, concat(job,'',marital,'',education) fullname, concat_ws(' ',job,marital,education) fullname1 from tab")
    //val res = spark.sql("select *, concat(job,'',marital,'',education) fullname, concat_ws(' ',job,marital,education) fullname1 from tab")

//****** Example 1 Concatenate
//val res = df.withColumn("fullname", concat_ws("_",$"marital",$"job",$"education"))
   //   .withColumn("fullname1",concat($"marital", lit("-"),$"job", lit("_"),$"education"))

// Result       Set - |57 |bluecollar  |married |primary  |no     |52     |yes    |no  |unknown|5  |may  |38      |1       |-1   |0       |unknown |no |



    //**** Example 2 RegexReplace
    //val res = df.withColumn("job", regexp_replace($"job","-", ""))
    //  .withColumn("marital",regexp_replace($"marital","single","Bachelor"))
    // .withColumn("marital",regexp_replace($"marital","divorced","Sepearted")

    // **** Example 2 Use "when" to replace multiple columns

    //  val res = df.withColumn("job", regexp_replace($"job","-", ""))
    // .withColumn("marital",regexp_replace($"marital","single","Bachelor"))
    // .withColumn("marital",when($"marital"=== "married","couple")
    // .when($"marital"=== "married","couple")
    //.when($"marital"=== "divorced","seperated")
    //.otherwise("marrital"))

    // **** Example 3 Use "Groupby"
   // with US-500
    // val res = df.groupBy($"state").count().orderBy($"count".desc)   //*** or use GG FUNCTION AND ADD A alias to the column
// with Bank-full csv
    // val res = df.groupBy($"marital").agg(count("*").alias("cnt")).orderBy($"cnt".desc)
   //val res = df.groupBy($"state").agg(max($"zip").as("cnt"))
   //val res =spark.sql("select state, city, collect_list(first_name) allnames from tab group by state, city")

    // Collect list gives an array of Data
  val res = df.groupBy($"state",$"city").agg(collect_list($"first_name").alias("names"))


// val res = df.groupBy($"state",$"city").agg(collect_set($"first_name").alias("names"))
    //************* Result Set
    // |state|city        |names                          |
    //+-----+------------+-------------------------------+
    //|IN   |Indianapolis|[Raymon, Malinda, Carey, Reita]|
    //|PA   |Jenkintown  |[Amber]


    res.show(10,false)
    res.printSchema()

    res.show(false)


    spark.stop()
  }
}