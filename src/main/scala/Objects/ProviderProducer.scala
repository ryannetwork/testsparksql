package Objects

import java.io.FileWriter

import scala.util.Random
import java.io.File

import Config.ConfigLoader
import org.apache.commons.io.FileUtils
object ProviderProducer {

  def main(args: Array[String]): Unit = {

    val jobConfigFile = args(0)
    println(jobConfigFile)
    val config = new ConfigLoader(jobConfigFile);
    val rnd = new Random()
    val filePath = config.ProvidersConfig.filePath
    //val destPath = ProvidersConfig.destPath

    //new File(filePath).delete()

    FileUtils.deleteQuietly(new File(filePath))
    val fw = new FileWriter(filePath, true)

    val header = "record_id,providerid,address1,address2,city,state,zip,phone\n"
    fw.write(header)

    for (iteration <- 1 to config.ProvidersConfig.records) {

      var providerid = fakers.fakerData.faker.number.digits(10)
      val phone = fakers.fakerData.faker.phoneNumber.cellPhone
      val address1 = fakers.fakerData.faker.address.streetAddress(false)
      val address2 = ""
      val city = fakers.fakerData.faker.address.city
      val state = fakers.fakerData.faker.address.stateAbbr
      val zip = fakers.fakerData.faker.address.zipCode

      val line = s"$iteration,$providerid,$address1,$address2,$city,$state,$zip,$phone\n"

      fw.write(line)

    }
    fw.close()

  }
}
