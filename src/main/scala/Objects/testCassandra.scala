package Objects

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._

object testCassandra  {

  case class Member (record_id: String,
                     memberid: String,
                     firstname: String,
                     lastname: String,
                     dob: String,
                     gender: String,
                     hicn: String,
                     address1: String,
                     city: String,
                     state: String,
                     zip: String,
                     phone: String)

  case class Members (
                       memberid: String,
                       firstname: String,
                       lastname: String,
                       dob: String,
                       gender: String,
                       hicn: String,
                       address1: String,
                       city: String,
                       state: String,
                       zip: String,
                       phone: String)


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val url = "jdbc:sqlserver://127.0.0.1:1433;DatabaseName=AdventureWorks2016CTP3;EnableBulkLoad=true;BulkLoadBatchSize=1000"
    val username = "sa"
    val password = "P@ssword12"

    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.local.dir", "c:\\tmp\\spark\\")
      .config("spark.executor.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val memberDS = spark.read.textFile("C:\\Projects\\spark\\testsparksql\\input\\memberSmallDataset.csv")
      .map(x => x.split(",")).map(m => Member(m(0),m(1),m(2),m(3),m(4),
      m(5),m(6),m(7),m(8),m(9),
      m(10),m(11)
    )).as[Member].cache()

    val test = memberDS
      .drop("record_id")
    test.as[Members]
    println(test.count())

    test.write.mode(SaveMode.Append)
      .cassandraFormat(keyspace = "testdb", table = "members")
      .save()

    val df = spark
      .read
      .cassandraFormat(keyspace = "testdb", table = "members")
      .load().as[Members].show(100)

    spark.stop()
    spark.close()

  }
}
