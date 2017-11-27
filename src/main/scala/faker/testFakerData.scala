package faker

object testFakerData {

  def main(args: Array[String]): Unit = {
    println(PhoneNumber.phoneNumber)
    println(Number.number(9))
    println(Code.isbn())
  }

}
