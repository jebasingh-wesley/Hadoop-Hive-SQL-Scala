package hadoop_hive_connection

import org.apache.spark.sql.SparkSession



object Hadoop_put extends App {

   val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExamples.com")
      .getOrCreate()


//  hadoop fs -chmod 600 h / user / hduser / hadoop /*
//  val dfmultiple=spark.read.option("header","true").option("inferSchema",true).csv("/home/ubuntu/workspace/data/airports.csv")



  val airportDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("/home/ubuntu/workspace/data/airports.csv")
  airportDF.printSchema()

//    airportDF.write
//        .format("parquet")
//        .option("header", "true")
//        .option("inferschema", "true")
//        .option("sep", ",")
//        .mode("overwrite")
//        .save("hdfs://localhost:9000/user/hadoop/")


}
