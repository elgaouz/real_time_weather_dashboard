import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object WeatherStreaming {

  def main(args: Array[String]): Unit = {

    println("Weather Monitoring Streaming with Kafka Demo Started ...")

    val KAFKA_TOPIC_NAME_CONS = "sample_topic"
    val KAFKA_BOOTSTRAP_SERVERS_CONS = "kafka:9092"

    // Needed when writing to HDFS-like paths from Spark
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val spark = SparkSession.builder
      .appName("Spark Structured Streaming with Kafka Demo")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // ===== Stream from Kafka =====
    val weather_detail_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "latest")
      .load()

    println("Printing Schema of weather_detail_df:")
    weather_detail_df.printSchema()

    // Cast Kafka value to string and keep Kafka timestamp
    val weather_detail_df_1 = weather_detail_df.selectExpr(
      "CAST(value AS STRING)",
      "CAST(timestamp AS TIMESTAMP)"
    )

    // Define schema of incoming JSON (similar to the YouTube example)
    val transaction_detail_schema = new StructType()
      .add("CityName", StringType)
      .add("Temperature", DoubleType)
      .add("Humidity", IntegerType)
      .add("CreationTime", StringType)

    val weather_detail_df_2 = weather_detail_df_1
      .select(
        from_json(col("value"), transaction_detail_schema).as("weather_detail"),
        col("timestamp")
      )

    val weather_detail_df_3 = weather_detail_df_2.select(
      col("weather_detail.*"),
      col("timestamp")
    )

    // Add parsed CreationDate column for time‑based analysis
    val weather_detail_df_4 = weather_detail_df_3.withColumn(
      "CreationDate",
      to_timestamp(col("CreationTime"), "yyyy-MM-dd HH:mm:ss")
    )

    println("Printing Schema of weather_detail_df_4:")
    weather_detail_df_4.printSchema()

    val weather_detail_df_5 = weather_detail_df_4.select(
      col("CityName"),
      col("Temperature"),
      col("Humidity"),
      col("CreationTime"),
      col("CreationDate")
    )

    println("Printing Schema of weather_detail_df_5:")
    weather_detail_df_5.printSchema()

    // ===== Write to console for debugging (like in the video) =====
    val weather_detail_write_stream = weather_detail_df_5.writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("append")
      .option("truncate", "false")
      .format("console")
      .start()

    // ===== Write final result into HDFS / local path as CSV =====
    weather_detail_df_5.writeStream
      .format("csv") // can be "orc", "json", "csv", etc.
      .option("path", "/output/weather_detail")
      .option("checkpointLocation", "/output/weather_detail_checkpoint")
      .outputMode("append")
      .start()

    weather_detail_write_stream.awaitTermination()
    println("Weather Monitoring Streaming with Kafka Demo Completed.")
  }
}