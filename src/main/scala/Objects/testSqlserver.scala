
package Objects

import org.apache.log4j.{Level, Logger}
import java.sql.DriverManager

import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import java.util.Properties

object testSqlserver {
  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    //spark configurations
    val sparkSession = SparkSession.builder().appName("Simple Application")
      .master("local")
      //.config("", "")
      //.config("spark.sql.warehouse.dir", ".")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "2g")
      .config("spark.local.dir", "c:\\tmp\\spark\\")
      .getOrCreate()


    val jdbcDF = sparkSession.read.format("jdbc").
      option("url", "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks2016CTP3;").
      option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
      option("dbtable", "Person.Address").
      option("user", "sa").
      option("password", "P@ssword12").load()

    jdbcDF.show(10)

    sparkSession.stop()
    sparkSession.close()

  }
}