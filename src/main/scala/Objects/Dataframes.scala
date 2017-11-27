package Objects

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import Common.Utilities.Validations

import scala.io.Source


object Dataframes {


    //schema generating function reading from layout file
    def dynamicSchema(file : String): StructType ={
      val readData = Source.fromFile(file).getLines().filter(!_.startsWith("#"))
      val schema = readData.map(x=>x.split(";", -1)).map
        {value => StructField(value(1), dataType(value(4)))}
      val structType = StructType(schema.toSeq)
      //    println(structType.prettyJson)
      structType
    }

    //dataframe generation from input source using the schema generated
    def genDataFrame(sqlContext : SQLContext, inputLines : RDD[String], schema : StructType,
                      dataDelimiter: String, hasHeader : String): DataFrame ={

      //val rows = inputLines.map(line=>line.split(dataDelimiter).map(_.trim))
      val header = inputLines.first()
      //val data = inputLines.filter(rows=>rows!=header)

      if (hasHeader.trim.toLowerCase == "true")
      {
        val rowFields = inputLines.filter(rows=>rows!=header).map{line => line.split(dataDelimiter, -1)}
          .map{ array => Row.fromSeq(array.zip(schema.toSeq)
            .map{ case (value, struct) => Validations.convertTypes(value.trim, struct) })}
        val df = sqlContext.createDataFrame(rowFields, schema)
        df
      }
      else
        {

          val rowFields = inputLines.map{line => line.split(dataDelimiter, -1)}
            .map{ array => Row.fromSeq(array.zip(schema.toSeq)
              .map{ case (value, struct) => Validations.convertTypes(value.trim, struct) })}
          val df = sqlContext.createDataFrame(rowFields, schema)
          df

        }
    }

    //defining data type for schema according to layout
    def dataType(dataType : String) : DataType ={
      if(dataType.equalsIgnoreCase("int")){
        IntegerType
      }
      else if(dataType.equalsIgnoreCase("date")){
        DateType
      }
      else if(dataType.equalsIgnoreCase("float")){
        FloatType
      }
      else if(dataType.equalsIgnoreCase("double")){
        DoubleType
      }
      else{
        StringType
      }
    }
  }