package Objects

case class Members ( record_id: String,
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

case class UnparsableMember(originalMessage: String, exception: Throwable)

object Members {
  def apply(array: Array[String]): Members = {
    Members(
      record_id = array(0),
      memberid = array(1),
      firstname = array(2),
      lastname = array(3),
      dob = array(4),
      gender = array(5),
      hicn = array(6),
      address1 = array(7),
      address2 = array(8),
      city = array(9),
      state = array(10),
      zip = array(11),
      phone = array(12))
  }

  def tryParserMember(logLine: String): Either[UnparsableMember, Members] = {
    import scala.util.control.NonFatal
    logLine.split(',') match {
      case Array(record_id, memberid, firstname, lastname, dob, gender, hicn,address1, address2, city, state, zip,  phone) =>

        try {
              Right(Members(record_id, memberid, firstname, lastname, dob, gender, hicn,address1, address2, city, state, zip,  phone))
        } catch {
          case NonFatal(exception) => Left(UnparsableMember(logLine, exception))
        }
      case _ => Left(UnparsableMember(logLine,
        new Exception("Unable to parser member")))
    }
  }

}