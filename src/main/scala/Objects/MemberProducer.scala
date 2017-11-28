package Objects
import java.io.FileWriter
import java.time.LocalDate
import java.util.concurrent.TimeUnit

import Config.Settings

import scala.util.Random
object MemberProducer extends App {
    // WebLog config
    val FirstNames = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/firstName.csv")).getLines().toArray
    val LastNames = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/lastName.csv")).getLines().toArray

  val wlc = Settings.WebLogGen
    val genders = Array("F", "M")

    val rnd = new Random()
    val filePath = wlc.filePath
    val destPath = wlc.destPath

     val fw = new FileWriter(filePath, true)

      for (iteration <- 1 to wlc.records) {

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
