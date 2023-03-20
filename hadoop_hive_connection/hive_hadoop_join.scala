package hadoop_hive_connection


import org.apache.spark.sql.SparkSession

object hive_hadoop_join extends App {


  //hive --service metastore
  // first you need to run on the terminal
  val spark = SparkSession.builder()
    .appName("Java Spark Hive Example")
    .master("local[*]")
    .config("hive.metastore.uris", "thrift://localhost:9083")
    .enableHiveSupport()
    .getOrCreate()


//  val airportDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("/home/ubuntu/workspace/data/airports.csv")
//  airportDF.printSchema()

//  spark.sql("""create database Demo;""").show()
//  spark.sql("""show databases;""").show()

  spark.sql("""use Demo;""")

//  spark.sql("""CREATE TABLE IF NOT EXISTS airport
//              |  (AirportID string,Name string,Country string,FAA string,ICAO string ,Latitude double,Longitude double,Altitude int,Timezone double,DST string,Tz string)
//              |  PARTITIONED BY(city string)
//              |  ROW FORMAT DELIMITED
//              |  FIELDS TERMINATED BY ',';
//              |;""".stripMargin)
//  spark.sql("desc airport").show()
//spark.sql("LOAD DATA INPATH 'LOAD DATA INPATH '/user/hadoop/airports.csv' OVERWRITE INTO TABLE airport PARTITION (city='Papua New Guinea');")
//spark.sql("select * from airport").show(20)







}




