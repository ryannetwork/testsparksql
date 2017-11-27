package Objects
import org.apache.spark.sql.functions.udf
import org.apache.spark.broadcast._
import scala.util.Random
object CustomRules {

  def defaultGender = udf((gender: String) => if(gender == null) "U" else gender)
  def defaultDates = udf((date: String) => if(date == null) "2099-12-31" else date)
  def randomValue = udf((value: String) => "Jane")
}
