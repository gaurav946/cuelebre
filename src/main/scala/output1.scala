import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_timestamp, udf}

object output1  {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]").appName("test").getOrCreate()

    def str_sec = udf((s: String) => {
      val Array(hour, minute, second) = s.split(":")
      hour.toInt * 3600 + minute.toInt * 60 + second.toDouble.toInt
    })
    val startedStream= spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").load("C:\\Users\\gaura\\Downloads\\cuelebre\\src\\main\\resources\\started_streams.csv")
    startedStream.withColumn("time",str_sec(col("time"))).show(truncate = false)

    startedStream.createOrReplaceTempView("startedStream")
    val whatson=spark.read.format("csv").option("header","true").load("C:\\Users\\gaura\\Downloads\\cuelebre\\src\\main\\resources\\whatson.csv")
    whatson.createOrReplaceTempView("whatson")

    val output1=spark.sql("""select
                |startedStream.dt,
                |startedStream.time,
                |startedStream.device_name,
                |startedStream.house_number,
                |startedStream.user_id,
                |startedStream.country_code,
                |startedStream.program_title,
                |startedStream.season,
                |startedStream.season_episode,
                |startedStream.genre,
                |startedStream.product_type,
                |whatson.broadcast_right_start_date,
                |whatson.broadcast_right_end_date
                |from startedStream join whatson on
                |startedStream.house_number=whatson.house_number
                |
                |""".stripMargin)

    output1.write.format("csv").save("yourpath/")


  }






}
