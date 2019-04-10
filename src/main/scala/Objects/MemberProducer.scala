package Objects
import java.io.{File, FileWriter}
import java.time.LocalDate
import java.util.concurrent.TimeUnit

import Config.ConfigLoader
import org.apache.commons.io.FileUtils

import scala.util.Random
object MemberProducer  {

    def main(args: Array[String]): Unit = {
      val FirstNames =  scala.io.Source.fromInputStream(getClass.getResourceAsStream("/firstName.csv")).getLines().toArray
      val LastNames = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/lastName.csv")).getLines().toArray

      val genders = Array("F", "M")

      val rnd = new Random()
      val jobConfigFile = args(0)
      println(jobConfigFile)
      val config = new ConfigLoader(jobConfigFile);
      val filePath = config.MembersConfig.filePath

      FileUtils.deleteQuietly(new File(filePath))

      val fw = new FileWriter(filePath, true)

      val header = "record_id,memberid,firstName,lastName,dob,gender,hicn,address1,address2,city,state,zip,phone\n"
      fw.write(header)

      for (iteration <- 1 to config.MembersConfig.records) {

        var memberid = (rnd.alphanumeric take 12 mkString("")).toUpperCase
        var hicn = (rnd.alphanumeric take 12 mkString("")).toUpperCase
        val gender = genders(rnd.nextInt(genders.length))
        val lastName = LastNames(rnd.nextInt(LastNames.length - 1))
        //val firstName = FirstNames(rnd.nextInt(FirstNames.length - 1))
        val firstName = fakers.fakerData.faker.name.firstName()
        val phone = fakers.fakerData.faker.phoneNumber.cellPhone

        val address1 = fakers.fakerData.faker.address.streetAddress(false)
        val address2 = ""
        val city =  fakers.fakerData.faker.address.city
        val state = fakers.fakerData.faker.address.stateAbbr
        val zip   =fakers.fakerData.faker.address.zipCode

        val dob = fakers.fakerData.dob

        val line = s"$iteration,$memberid,$firstName,$lastName,$dob,$gender,$hicn,$address1,$address2,$city,$state,$zip,$phone\n"

        fw.write(line)

      }
      fw.close()

    }

    }
