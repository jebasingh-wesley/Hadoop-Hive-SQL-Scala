package hadoop_hive_connection

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.net.URI


object hadoop_creating_folder extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val conf = new Configuration()
  val fs = FileSystem.get(new URI("hdfs://localhost:9000/"), conf)

  val directoryPath = new Path("hdfs://localhost:9000/user/hadoop/")

  if (!fs.exists(directoryPath)) {
    fs.mkdirs(directoryPath)
    println("Directory created successfully!")
  } else {
    println("Directory already exists!")
  }

  spark.stop()






}
