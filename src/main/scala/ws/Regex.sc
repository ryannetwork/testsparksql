import scala.util.matching.Regex

  val zipPattern = "(\\d{5})" //5 digt number


val number = "\\d+" //empty or number
val name = "[A-Za-z0-9_']+\\s?"
val date = "^\\d{4}\\-(0?[1-9]|1[012])\\-(0?[1-9]|[12][0-9]|3[01])$"
val gentder = "^(?:m|M|male|Male|f|F|female|Female)$"
val zip = "^\\d{5}-\\d{4}|\\d{5}|[A-Z]\\d[A-Z] \\d[A-Z]\\d$"

println("30014".matches(zip))
println("F".matches(gentder))
//println("1973a-03-12".matches(date))
//println("o'bien&".matches(name))
//println("".matches(number))
//println("30034".matches(zipPattern))