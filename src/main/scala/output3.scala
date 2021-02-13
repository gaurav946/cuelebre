import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object output3 {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]").appName("test").getOrCreate()


    //converting time to seconds using udf
    def str_sec = udf((s: String) => {
      val Array(hour, minute, second) = s.split(":")
      hour.toInt * 3600 + minute.toInt * 60 + second.toDouble.toInt
    })


    val startedStream= spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter",";").load("C:\\Users\\gaura\\Downloads\\cuelebre\\src\\main\\resources\\started_streams.csv")

    startedStream.withColumn("time",str_sec(col("time"))).createOrReplaceTempView("startedStream")

    val mostPopularGenre=spark.sql(
      """
        |select count(genre) as totalgenre,genre from startedStream
        | group by genre
        |  order by totalgenre desc limit 1
        |""".stripMargin)

    mostPopularGenre.createOrReplaceTempView("mostPopularGenre")



   val output3= spark.sql(
      """select
        |first(genre) as genre,
        |sum(time)/3600 as watched_hour,
        |user_id as unique_users
        |from startedStream
        |where genre=(select genre from mostPopularGenre)
        |group by user_id""".stripMargin)

    output3.write.format("csv").save("yourpath/")






  }


}
