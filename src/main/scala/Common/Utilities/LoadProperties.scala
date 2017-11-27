package Common.Utilities

import scala.io.Source

object LoadProperties {

  //read the properties file parsing with "=" and neglecting the comments and returns a map
  def readPropertiesToMap(file : String): Map[String, String] ={
    //val readData = Source.fromInputStream(getClass.getResourceAsStream(file))
    val readData = Source.fromFile(file)
    val filteredLines = readData.getLines().filter(!_.startsWith("#")).filter(!_.isEmpty)
    val mapData = filteredLines.map(line=>line.split("=")).map(kv => (kv(0).trim -> kv(1).trim)).toMap
    mapData
  }
}
