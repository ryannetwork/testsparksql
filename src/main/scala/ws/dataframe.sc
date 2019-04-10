
import org.apache.spark.sql.SparkSession
import fakers._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

val spark = SparkSession
  .builder()
  .appName("testdf")
  .config("spark.local.dir", "c:\\tmp\\spark\\")
  .config("spark.executor.memory", "2g")
  .config("spark.cassandra.connection.host", "localhost")
  .master("local")
  .getOrCreate()

val financesDF = spark.read.json("C:\\Projects\\spark\\testsparksql\\Data\\finances-small.json")
financesDF.show()

import spark.implicits._  //use implicite to acces column name $ or '

financesDF
  .na.drop("all", Seq("ID","Account","Amount","Description","Date")) //drop if data not avaialabe
  .na.fill("Unknown", Seq("Description")) //fill default "unknown if null"
  .where($"Amount" =!= 0 || $"Description" === "Unknown")
  .selectExpr("Account.Number as AccountNumber", "Amount", "Date", "Description") //specify column to return
   .show()

financesDF.createOrReplaceTempView("finances") //create temp vie for spark sql
val df3 = spark.sql("select * from finances")
  df3.show()





