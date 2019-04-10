import org.apache.spark.sql.SparkSession
import fakers._

import org.apache.spark.sql._
import Common.Utilities._
import org.apache.spark.sql.functions.udf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

val spark = SparkSession
  .builder()
  .appName("Spark SQL data sources example")
  .config("spark.local.dir", "c:\\tmp\\spark\\")
  .config("spark.executor.memory", "2g")
  .config("spark.cassandra.connection.host", "localhost")
  .master("local")
  .getOrCreate()
