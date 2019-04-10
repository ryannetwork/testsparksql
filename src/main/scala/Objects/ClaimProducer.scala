package Objects
import java.io.{File, FileWriter}

import Config.ConfigLoader
import org.apache.commons.io.FileUtils

import scala.io.Source
import scala.util.Random

object ClaimProducer {

  def main(args: Array[String]): Unit = {

    val jobConfigFile = args(0)
    println(jobConfigFile)
    val config = new ConfigLoader(jobConfigFile);

    val memberFile = config.ClaimsConfig.memberFilePath
    val providerFile =  config.ClaimsConfig.providerFilePath
    val member = Source.fromFile(config.ClaimsConfig.memberFilePath).getLines().map(x=>x.split(",")).map(x => x(1)).toArray
    val provider = Source.fromFile(config.ClaimsConfig.providerFilePath).getLines().map(x=>x.split(",")).map(x => x(1)).toArray
    val POSs = Array("11", "12", "13", "14", "15")
    val rnd = new Random()

    val filePath = config.ClaimsConfig.filePath
    FileUtils.deleteQuietly(new File(filePath))

    val fw = new FileWriter(filePath, true)

    val header = "record_id,memberid,providerid,claimid,pos\n"
    fw.write(header)

    val memberLen = member.length - 1
    val providerLen = provider.length - 1
    val records =  config.ClaimsConfig.records
    println ("total Member: " + memberLen);
    println ("total Provider: " + providerLen);

    for (iteration <- 1 to records ) {

      var claimid = fakers.fakerData.faker.number().digits(10)
      val memberid = member(rnd.nextInt(memberLen))
      val providerid = provider(rnd.nextInt(providerLen))
      val pos = POSs(rnd.nextInt(POSs.length))
      val line = s"$iteration,$memberid,$providerid,$claimid,$pos\n"

      fw.write(line)

    }
    fw.close()

  }



}
