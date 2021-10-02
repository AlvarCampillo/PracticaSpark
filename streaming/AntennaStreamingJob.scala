package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object AntennaStreamingJob extends StreamingJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "34.125.80.173")
      .option("subscribe", "devices")
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val antennaMessageSchema: StructType = ScalaReflection.schemaFor[AntennaMessage].dataType.asInstanceOf[StructType]
    dataFrame
      .select(from_json(col("bytes"), antennaMessageSchema).as("json"))
      .select("json.*")
      .withColumn("str", explode(
        array(
          struct($"id", $"timestamp".cast(TimestampType), lit("user_total_bytes").as("type")),
          struct($"antenna_id".as("id"), $"timestamp".cast(TimestampType), lit("antenna_total_bytes").as("type")),
          struct($"app".as("id"), $"timestamp".cast(TimestampType), lit("app_total_bytes").as("type"))
        )
      ))
      .select($"bytes", $"str.*")
  }

  override def computeByteCountByType(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"bytes", $"str.*")
      .withWatermark("timestamp", "1 minute")
      .groupBy($"id", window($"timestamp", "5 minutes"))
      .agg(
        $"timestamp", $"bytes", $"type"
      )
      .select($"bytes", $"window.start".as("timestamp"), $"type", $"id")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", s"jdbc:postgresql://35.232.169.53:5432/postgres")
          .option("dbtable", "bytes")
          .option("user", "postgres")
          .option("password", "keepcoding")
          .save()
      }
      .start()
      .awaitTermination()
  }


  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    val columns = dataFrame.columns.map(col).toSeq ++
      Seq(
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )

    dataFrame
      .select(columns: _*)
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", "Users/alvar/Desktop/BD, IA y ML/big-data-processing/final-exercise/solution/src/main/scala/io/keepcoding/spark/exercise/data")
      .option("checkpointLocation", "Users/alvar/Desktop/BD, IA y ML/big-data-processing/final-exercise/solution/src/main/scala/io/keepcoding/spark/exercise/data/checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = run(args)
}
