package Objects
import com.datastax.spark.connector.SomeColumns
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

case class Member (
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


object MemberStream {

  def main(args: Array[String]) {

    val localLogger = Logger.getLogger("MemberStream")


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[5]")
      .appName("MemberStream")
      .config("spark.driver.memory", "2g")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()


    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val kafkaTopicRaw = "test"
    val kafkaBroker = "127.0.01:9092"

    val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBroker)
    localLogger.info(s"connecting to brokers: $kafkaBroker")
    val topicLines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)


    topicLines.map{lines =>
      println("===============================================================")
      (lines._1+"----"+lines._2)
    }.print()


    //val parsedMemberStream = topicLines.map( x => x._2.split(","))
    import com.datastax.spark.connector.streaming._

    val parsedMemberStream = topicLines.map(keyVal => Members.tryParserMember(keyVal._2))
      .flatMap(_.right.toOption)

    /*
    parsedMemberStream.foreachRDD { row =>
      import spark.implicits._

      if (row.count() > 0) {
        row.toDF("record_id", "memberid", "firstname", "lastname", "dob", "gender", "hicn", "address1", "address2", "city", "state", "zip", "phone")
          .drop("record_id")
          .as[Member] //.show()
          .write.mode(SaveMode.Append)
          .cassandraFormat(keyspace = "testdb", table = "members")
          .save()

      }
    }
    */
    parsedMemberStream.saveToCassandra("testdb", "members", SomeColumns("memberid", "firstname", "lastname", "dob", "gender", "hicn", "address1", "address2", "city", "state", "zip", "phone"))

    //Kick off
    ssc.start()

    ssc.awaitTermination()


  }

}