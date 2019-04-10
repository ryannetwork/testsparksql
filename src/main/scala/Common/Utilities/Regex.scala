package Common.Utilities

object Regex {
    val INT_PATTERN: String = "^[-+]?[0-9]*$"
    val FLOAT_PATTERN: String = "^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$"
    val DATE_PATTERN: String = "^(\\d{4})([-])(0[1-9]|1[0-2])([-])([12]\\d|0[1-9]|3[01])(\\D?([01]\\d|2[0-3])\\D?([0-5]\\d)\\D?([0-5]\\d)?\\D?(\\d{3})?)?$"

    implicit class RegExHelper(value: String){
        import scala.util.matching.Regex
        def isValidFormat(pattern: String)= value.matches(pattern)

    }
}
