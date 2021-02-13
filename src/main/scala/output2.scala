import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object output2 {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().master("local[*]").appName("test").getOrCreate()

    val startedStream= spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").load("C:\\Users\\gaura\\Downloads\\cuelebre\\src\\main\\resources\\started_streams.csv")

    startedStream.createOrReplaceTempView("startedStream")


    val output2=spark.sql(
      """select first(dt) as dt ,
        |first(device_name) as device_name,
        |first(country_code) as country_code,
        |first(product_type) as product_type,
        |first(program_title) as program_title,
        |user_id as unique_users,
        |count(program_title) as content_count
        | from
        | startedStream
        |group by user_id,program_title""".stripMargin)

    output2.write.format("csv").save("yourpath/")

  }

}
