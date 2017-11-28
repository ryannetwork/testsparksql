package Objects

import fakers._

import org.apache.spark.sql._
import Common.Utilities._
import org.apache.spark.sql.functions.udf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object MainEntry {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    if (args.length < 1) {
      System.err.println(
        "Usage: filename.jar filepath.jobcfg")
      System.exit(1)
    }



    //    val clientId = args(0)+"/"
    //reading clientConfig
    //val clientConfigFile = "/client_config.properties"
    //reading jobconfig for input output recordtypes etc
    val jobConfigFile = args(0) // "/validation_eligibility.jobcfg"

    println(jobConfigFile)


    //loading the properties to map
    //val clientConfigProps = LoadProperties.readPropertiesToMap(clientConfigFile)
    val jobConfigProps = LoadProperties.readPropertiesToMap(jobConfigFile)

    //defining variables
    val sourceFile = jobConfigProps("inputFile")

    println(sourceFile)


    val dataDelimiter = jobConfigProps("dataDelimiter")
    val lineDelimiter = jobConfigProps("lineDelimiter")
    val hasHeader  = jobConfigProps("hasHeader")
    val outputFile = jobConfigProps("outputDirectory")
    val outputFileIntMemberId = jobConfigProps("outputIntMemberId")

    val sourceLayoutFile = jobConfigProps("inputLayoutFile")

    //spark configurations
    val sparkSession = SparkSession.builder().appName("Simple Application")
      .master("local")
      //.config("", "")
      //.config("spark.sql.warehouse.dir", ".")
      .config("spark.sql.warehouse.dir", "C:/tmp")
      .config("spark.local.dir", "c:/tmp")
       .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "2g")
      .getOrCreate()

    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    //schema generation for the input source
    val schema = Dataframes.dynamicSchema(sourceLayoutFile)
//    println(schema.prettyJson)
    println(sourceFile)

    //sc.hadoopConfiguration.set("textinputformat.record.delimiter", "\n")
    val sourceDataRdd = sc.textFile(sourceFile)
    //println(sourceDataRdd.count())
    //sourceDataRdd.foreach(println)

    val eligibilityDF = Dataframes.genDataFrame(sqlContext, sourceDataRdd, schema,  dataDelimiter,hasHeader)
                      .cache()
    eligibilityDF.show()

   // eligibilityDF
   //   .na.drop("all")
   //   .na.fill("U",Seq("gender")).show(false) // empty column in gender column fill with unknown
    /*
    val firstnames = sc.textFile("/Users/ryannguyen/ScalaProjects/sparksql-examples/src/main/resources/firstNameRedact.csv")
                        .collect().toList;

    val rnd = new Random()
     val bcFirstNames = sc.broadcast(firstnames)

    val randValue = udf((value: String) => {
      bcFirstNames.value(rnd.nextInt(bcFirstNames.value.length))
      	})
  */

    eligibilityDF.createOrReplaceTempView("Members");
    val df2 = sparkSession.sql("SELECT * FROM Members")
    df2.show()

    val randFirstName = udf((value: String) => fakerData.faker.name.firstName)
    val randLastName = udf((value: String) => fakerData.faker.name.lastName)
    val randPhone = udf((value: String) => fakerData.faker.phoneNumber.phoneNumber.filter(_.isDigit))
    val randAddress1 = udf((value: String) => fakerData.faker.address.streetAddress(false))
    val randCity = udf((value: String) => fakerData.faker.address.city)
    val randState = udf((value: String) => fakerData.faker.address.stateAbbr)
    val randZip = udf((value: String) => fakerData.faker.address.zipCode)
    val rndMemberId = udf((value: String) => fakerData.faker.code.isbn13)


    //TO_DATE(CAST(UNIX_TIMESTAMP("dob", ' yyyy-MM-dd') AS TIMESTAMP))

    val df3 = eligibilityDF.filter(eligibilityDF("dob").isNotNull && eligibilityDF("hicn").isNotNull)
      .withColumn("Year",year($"dob"))
      .withColumn("month", month($"dob"))
       .where('year > 1970 )
      .sample(false, 0.01, 0)
          .withColumn("gender", CustomRules.defaultGender(eligibilityDF.col("gender")))
          .withColumn("firstname", randFirstName(eligibilityDF("firstname") ))
            .withColumn("lastName",   randLastName(eligibilityDF("lastname") ))
              .withColumn("address1", randAddress1(eligibilityDF("address1") ))
              .withColumn("city",         randCity(eligibilityDF("city") ))
              .withColumn("state",       randState(eligibilityDF("state") ))
              .withColumn("zip",           randZip(eligibilityDF("zip") ))
              .withColumn("phone",       randPhone(eligibilityDF("phone") ))
              .withColumnRenamed("record_id", "rowNum")
              .withColumn("memberid_new",       rndMemberId(eligibilityDF("memberid") ))
               .withColumnRenamed("memberid", "memberid")
              .drop("address2")

    df3.show()

    df3
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .option("delimiter", ",")
        .option("header", true)
        .csv(outputFile)
    // }

    //stopping sparkContext
    sc.stop()


    //val path = getClass.getResource("/")
    //val folder = new File(path.getPath)
    //if (folder.exists && folder.isDirectory)
    //  folder.listFiles
    //    .toList
    //    .foreach(file => println(file.getName))
  }

  implicit class DataFrameHelper(df:DataFrame){
    import scala.util.Try
    def hasColumn(columnName: String)=Try(df(columnName)).isSuccess
  }
}


