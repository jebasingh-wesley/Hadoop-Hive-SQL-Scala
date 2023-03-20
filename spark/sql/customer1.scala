package spark.sql

import org.apache.spark.sql._;
import org.apache.spark._;
import java.sql.Connection
import java.io.FileInputStream
import java.util.Properties
object customer1 {
def main(args:Array[String])//args("hdfs://localhost:54310/user/hduser/oozie/4spark/cust","/home/hduser/install/oozie_updated/oozieusecases/4spark/connection.prop")
{
val spark=SparkSession.builder().appName("Spark job submission and Cron")//.master("local[*]")
.config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
.config("spark.eventLog.dir", "file:////tmp/spark-events")
.config("spark.eventLog.enabled", "true") // 3 configs are for storing the events or the logs in some location, so the history can be visible
.config("spark.sql.shuffle.partitions",10) //Priority or config - top hardcodes in code, next for parameters, last priority for default/environment setting
.config("spark.sql.hive.metastore.version","2.3.0")
.config("spark.sql.hive.metastore.jars","/usr/local/hive/lib/*")
.enableHiveSupport().getOrCreate();

val df1=spark.read.option("inferschema","true").option("header", "true").csv(args(0)).repartition(4)
df1.show(10,false);

val conn=new Properties()
val propFile= new FileInputStream(args(1))
conn.load(propFile)
 //Reading mysql server connection detail from property file 
val User =conn.getProperty("user")
val Pass =conn.getProperty("pass")
val Url =conn.getProperty("url")
val Dbtable =conn.getProperty("dbtable")
val Driver =conn.getProperty("driver")
val Hivetable =conn.getProperty("hivetable")

val prop=new java.util.Properties();
prop.put("user", User)
prop.put("password", Pass)
prop.put("driver", Driver)
df1.write.mode("append").jdbc(Url,Dbtable,prop)

df1.write.mode("append").saveAsTable(Hivetable)
}
}








