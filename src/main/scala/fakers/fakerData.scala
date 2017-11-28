package fakers

import java.util.Date
import java.util.concurrent.TimeUnit

import org.joda.time.DateTime

import scala.util.Random

object fakerData
  extends BasedFaker {

  val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")

  def futureDate(day: Int): DateTime =
    new DateTime(faker.date.future(day, TimeUnit.SECONDS, new Date))

  def futureDateRandom: DateTime =
    new DateTime(faker.date.future(21 - Random.nextInt(20)
      , TimeUnit.SECONDS, new Date))

  def dob: String = dateFormat.format(faker.date.birthday())

  def email: String = faker.internet.emailAddress()

}

