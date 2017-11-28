package Objects
import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object testSqlserver2  {

  case class Member (record_id: String,
                     memberid: String,
                     firstname: String,
                     lastname: String,
                     dob: String,
                     gender: String,
                     hicn: String,
                     address1: String,
                     address2: String,
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
      .config("spark.some.config.option", "some-value")
      .config("spark.driver.memory", "3")
      .master("local")
      .getOrCreate()
    import spark.implicits._
     val memberDS = spark.read.textFile("C:\\Projects\\spark\\testsparksql\\input\\data.csv")
          .map(x => x.split(",")).map(m => Member(m(0),m(1),m(2),m(3),m(4),
       m(5),m(6),m(7),m(8),m(9),
       m(10),m(11),m(12)
                )).as[Member].cache()

    val prop = new java.util.Properties
    prop.setProperty("driver", driver)
    prop.setProperty("user", username)
    prop.setProperty("password", password)

    println("First method : directly from dataset")
    val timeBefore = java.lang.System.currentTimeMillis()

    memberDS.write.mode("append").jdbc(url, "dbo.Member", prop)
    val timeAfter = java.lang.System.currentTimeMillis()
    val timeDifference = timeAfter - timeBefore

    println("TIME difference:" + timeDifference)


    val conn = DriverManager.getConnection(url, username, password)
    val statement = conn.createStatement()
    conn.setAutoCommit(false)
    val stmt = conn.prepareStatement("insert into dbo.Member  (record_id, memberid, firstname, lastname, dob, gender,  " +
      "hicn, address1, address2, city, state,zip, phone) " +
      "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    memberDS.collect().foreach{ ds =>
      stmt.setString(1, ds.record_id)
      stmt.setString(2, ds.memberid)
      stmt.setString(3, ds.firstname)
      stmt.setString(4, ds.lastname)
      stmt.setString(5, ds.dob)
      stmt.setString(6, ds.gender)
      stmt.setString(7, ds.hicn)
      stmt.setString(8, ds.address1)
      stmt.setString(9, ds.address2)
      stmt.setString(10, ds.city)
      stmt.setString(11, ds.state)
      stmt.setString(12, ds.zip)
      stmt.setString(13, ds.phone)
      stmt.addBatch()
    }

    println("Method: Insert statements in a multiple batches")
    val timeBefore2 = java.lang.System.currentTimeMillis()
    stmt.executeBatch()
    conn.commit()
    val timeAfter2 = java.lang.System.currentTimeMillis()
    val timeDifference2 = timeAfter2 - timeBefore2
    println("TIME difference: " + timeDifference2)



  }
}
