package hadoop_hive_connection


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.net.URI


object hadoop_size_check extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val conf = new Configuration()
  val fs = FileSystem.get(new URI("hdfs://localhost:9000/"), conf)
  val directoryPath = new Path("hdfs://localhost:9000/user/hadoop/")

  val folderSize = fs.getContentSummary(directoryPath).getLength()
  println(folderSize)

//  val newPermission = FsPermission.valueOf("drwxr-xr-x")
//  fs.setPermission(directoryPath, newPermission)

  spark.stop()






}


