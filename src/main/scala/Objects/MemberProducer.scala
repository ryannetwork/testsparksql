package Objects
import java.io.FileWriter
import java.time.LocalDate
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
        val firstName = FirstNames(rnd.nextInt(FirstNames.length - 1))
        val phone = faker.PhoneNumber.phoneNumber

    //    #8;address1;address1;address1;string;;
    //    #9;address2;address2;address2;string;;
    //    #10;city;city;city;string;;
    //    #11;state;state;state;string;;
    //    #12;zip;zip;zip;string;;

        val address1 = faker.Address.streetAddress(false)
        val address2 = ""
        val city = faker.Address.city
        val state = faker.Address.stateAbbr
        val zip   = faker.Address.zipCode


        val dob =
          LocalDate.now.minusYears(Random.nextInt(50)).toString


        val line = s"$iteration,$memberid,$firstName,$lastName,$dob,$gender,$hicn,$address1,$address2,$city,$state,$zip,$phone\n"

        fw.write(line)

      }
      fw.close()

    }
