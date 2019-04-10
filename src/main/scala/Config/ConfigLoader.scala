package Config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import java.io.File
class ConfigLoader (val configFile: String)  {

  //private val config = ConfigFactory.load()
  private val config =  ConfigFactory.parseFile(new File(configFile))

  object SmallDataConfig {


   private val smallDatasetConfig = config.getConfig("smallDataset")

    lazy val memberlayout = smallDatasetConfig.getString("memberlayout")
    lazy val claimdata = smallDatasetConfig.getString("claimdata")
    lazy val memberdata = smallDatasetConfig.getString("memberdata")
    lazy val providerdata = smallDatasetConfig.getString("providerdata")
    lazy val hasHeader = smallDatasetConfig.getString("hasHeader")
    lazy val dataDelimiter = smallDatasetConfig.getString("dataDelimiter")
    lazy val lineDelimiter = smallDatasetConfig.getString("lineDelimiter")
    lazy val outputDirectory = smallDatasetConfig.getString("outputDirectory")
    lazy val ErrorOutput = smallDatasetConfig.getString("ErrorOutput")
    lazy val samplePercent = smallDatasetConfig.getDouble("samplePercent")

  }


  object MembersConfig {
    private val memConfig = config.getConfig("membersProducer")

    lazy val records = memConfig.getInt("records")
    lazy val filePath = memConfig.getString("file_path")
    lazy val destPath = memConfig.getString("dest_path")
    lazy val numberOfFiles = memConfig.getInt("number_of_files")

  }

  object ProvidersConfig {
    private val memConfig = config.getConfig("providersProducer")

    lazy val records = memConfig.getInt("records")
    lazy val filePath = memConfig.getString("file_path")
    lazy val destPath = memConfig.getString("dest_path")
    lazy val numberOfFiles = memConfig.getInt("number_of_files")

  }

  object ClaimsConfig {
    private val claimConfig = config.getConfig("claimsProducer")

    lazy val records = claimConfig.getInt("records")
    lazy val filePath = claimConfig.getString("file_path")
    lazy val destPath = claimConfig.getString("dest_path")
    lazy val memberFilePath = claimConfig.getString("memberFilePath")
    lazy val providerFilePath = claimConfig.getString("providerFilePath")
    lazy val numberOfFiles = claimConfig.getInt("number_of_files")



  }

  object KafkaConfig {
    private val kafkaConfig = config.getConfig("membersProducer")
    lazy val kafkaServer: String = kafkaConfig.getString("kafka.server")
    lazy val kafkaTopic: String = kafkaConfig.getString("kafka.topic")
    lazy val kafkaClientId: String = kafkaConfig.getString("kafka.clientId")
  }

}

/*
object ConfigLoader {

  val configFactory: Config = ConfigFactory.load()

  val sparkAppName: String = configFactory.getString("spark.app.name")
  val sparkMaster: String = configFactory.getString("spark.ip")
  val sparkCores: String = configFactory.getString("spark.cores")
  val sparkMemory: String = configFactory.getString("spark.executor.memory")


  val zookeeperUrl: String = configFactory.getString("zookeeper.server")

  val sparkConf: SparkConf = new SparkConf()
    .setAppName(sparkAppName)
    .setMaster(sparkMaster)
    .set("spark.executor.memory", sparkMemory)
    .set("spark.executor.core", sparkCores)
}

*/