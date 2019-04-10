package Objects

import fakers._
import org.apache.spark.sql._
import Common.Utilities._
import Config.ConfigLoader
import org.apache.spark.sql.functions.udf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SmallDataset {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    if (args.length < 1) {
      System.err.println(
        "Usage: filename.jar filepath.jobcfg")
      System.exit(1)
    }

    val jobConfigFile = args(0)
    println(jobConfigFile)
    val config = new ConfigLoader(jobConfigFile);

    val dataDelimiter = config.SmallDataConfig.dataDelimiter; // jobConfigProps("dataDelimiter")
    val lineDelimiter = config.SmallDataConfig.lineDelimiter // jobConfigProps("lineDelimiter")
    val hasHeader  =  config.SmallDataConfig.hasHeader // jobConfigProps("hasHeader")
    val outputFile = config.SmallDataConfig.outputDirectory //jobConfigProps("outputDirectory")
    val samplePercent = config.SmallDataConfig.samplePercent / 100.0

    val ErrorOutput = config.SmallDataConfig.ErrorOutput //jobConfigProps("outputDirectory")

    val memberlayout = config.SmallDataConfig.memberlayout
    val claimdata = config.SmallDataConfig.claimdata
    val memberdata = config.SmallDataConfig.memberdata
    val providerdata = config.SmallDataConfig.providerdata

    //spark configurations
    val sparkSession = SparkSession.builder().appName("SmallDataset")
      .master("local")
      //.config("", "")
      //.config("spark.sql.warehouse.dir", ".")
      .config("spark.local.dir", "c:\\tmp\\spark\\")
       .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "2g")
      .getOrCreate()

    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    sparkSession.sqlContext.udf.register("strLen", (s: String) => s.length())
    sparkSession.sqlContext.udf.register("rndValue", rndValue _) //_ is passing param


    //println(memberlayout)
    val schemaMember = Dataframes.dynamicSchema(memberlayout)
    //println(schemaMember.prettyJson)

    val memberDataRdd = sc.textFile(memberdata)
    val memberDF = Dataframes.genDataFrame(sqlContext, memberDataRdd, schemaMember,  dataDelimiter,hasHeader)
      .cache()
    memberDF.show(20)


    memberDF.filter($"invalidData" === 1).show()
      //.write.option("header", "true")
      //  .mode(SaveMode.Overwrite).csv(ErrorOutput + "Member\\")

    memberDF
          .filter("invalidData is null")
          .createOrReplaceTempView("Members");
/*
    memberDF.createOrReplaceTempView("Members");
    val df2 = sparkSession.sql("SELECT record_id, memberid as memberid_old, rndValue('memberid') as memberid, firstname as firstname_old," +
      " rndValue('firstname') as firstname FROM Members")
    df2.show()
*/

    val dfClaim = sparkSession.read.option("header","true").csv(claimdata).cache()
   //   dfClaim.show(5)

      val dfClaimSmall = dfClaim.sample(true, samplePercent).cache()

    println("total Sample Claim Records: " + dfClaimSmall.count() )

    val dfProvider = sparkSession.read.option("header","true").csv(providerdata).cache()

     dfProvider.createOrReplaceTempView("Providers");
     dfClaimSmall.createOrReplaceTempView("Claims");

    //https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-joins.html
     //dfClaimSmall.join(memberDF, $"memberid" === $"memberid" ).show()

    val dfMemberSmall =  sparkSession.sql("select m.record_id, m.memberid as memberid_old, " +
                                                 "rndValue('memberid') as memberid," +
      "rndValue('firstname') as firstname," +
      "rndValue('lastname') as lastname," +
      "rndValue('dob') as dob," +
      "m.gender," +
      "rndValue('hicn') as hicn," +
      "rndValue('address') as address1," +
      "rndValue('city') as city," +
      "rndValue('state') as state," +
      "rndValue('zip') as zip," +
      "rndValue('phone') as phone" +
       " from Members m, (select distinct memberid from Claims) c where m.memberid = c.memberid").cache()

    println("smallset member")
    dfMemberSmall.show(2)

    val dfProviderSmall =  sparkSession.sql("select p.record_id,   p.providerid as providerid_old," +
      "rndValue('providerid') as providerid," +
      "rndValue('address') as address1," +
      "rndValue('city') as city," +
      "rndValue('state') as state," +
      "rndValue('zip') as zip," +
      "rndValue('phone') as phone" +
      " from Providers p, (select distinct Providerid from Claims) c where p.Providerid = c.Providerid").cache()

    println("smallset provider")
    dfProviderSmall.show(2)

    dfClaimSmall.join(dfMemberSmall, dfClaimSmall("memberid") === dfMemberSmall("memberid_old"))
        .join(dfProviderSmall, dfClaimSmall("providerid") === dfProviderSmall("providerid_old"))
              .select(
                dfClaimSmall("record_id"),
                dfMemberSmall("memberid"),
                      dfProviderSmall("providerid"),
                      dfClaimSmall("claimid"),
                      dfClaimSmall("pos"))
          .coalesce(1)
          .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(outputFile + "\\claimsSmalldata")

    dfProviderSmall.drop("providerid_old")
      .coalesce(1)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(outputFile + "\\providerSmalldata")

    dfMemberSmall.drop("memberid_old")
      .coalesce(1)
      .write
      .option("header", "true")
    .mode(SaveMode.Overwrite)
        .csv(outputFile + "\\memberSmalldata")
   // memberDF
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


    val randFirstName = udf((value: String) => fakerData.faker.name.firstName)
    val randLastName = udf((value: String) => fakerData.faker.name.lastName)
    val randPhone = udf((value: String) => fakerData.faker.phoneNumber.phoneNumber.filter(_.isDigit))
    val randAddress1 = udf((value: String) => fakerData.faker.address.streetAddress(false))
    val randCity = udf((value: String) => fakerData.faker.address.city)
    val randState = udf((value: String) => fakerData.faker.address.stateAbbr)
    val randZip = udf((value: String) => fakerData.faker.address.zipCode)
    val rndMemberId = udf((value: String) => fakerData.faker.code.isbn13)

    //TO_DATE(CAST(UNIX_TIMESTAMP("dob", ' yyyy-MM-dd') AS TIMESTAMP))

    /*
    val df3 = memberDF.filter(memberDF("dob").isNotNull && memberDF("hicn").isNotNull)
      .withColumn("Year",year($"dob"))
      .withColumn("month", month($"dob"))
       .where('year > 1970 )
      .sample(false, 0.01, 0)
          .withColumn("gender", CustomRules.defaultGender(memberDF.col("gender")))
          .withColumn("firstname", randFirstName(memberDF("firstname") ))
            .withColumn("lastName",   randLastName(memberDF("lastname") ))
              .withColumn("address1", randAddress1(memberDF("address1") ))
              .withColumn("city",         randCity(memberDF("city") ))
              .withColumn("state",       randState(memberDF("state") ))
              .withColumn("zip",           randZip(memberDF("zip") ))
              .withColumn("phone",       randPhone(memberDF("phone") ))
              .withColumnRenamed("record_id", "rowNum")
              .withColumn("memberid_new",       rndMemberId(memberDF("memberid") ))
               //.withColumnRenamed("memberid", "memberid")
              .drop("address2")
*/
    //df3.show()

    /*
    df3
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .option("delimiter", ",")
        .option("header", true)
        .csv(outputFile)
    // }
    */

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



  def rndValue (value: String ): String = value.toLowerCase match {
    case "firstname" => fakerData.faker.name.firstName
    case "lastname" => fakerData.faker.name.lastName
    case "address" =>  fakerData.faker.address.streetAddress(false)
    case "phone" => fakerData.faker.phoneNumber.phoneNumber //.filter(_.isDigit)
    case "city" => fakerData.faker.address.city
    case "state"  => fakerData.faker.address.stateAbbr
    case "zip"  => fakerData.faker.address.zipCode
    case "memberid" => fakerData.faker.code.isbn13
    case "providerid" => fakerData.faker.code.isbn13
    case "dob" => fakers.fakerData.dob
     case "hicn" => fakerData.faker.number().digits(9)
    case _ =>  null
  }
}


