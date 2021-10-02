package io.keepcoding.spark.exercise.batch

import java.time.OffsetDateTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AntennaBatchJob extends BatchJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark SQL KeepCoding Base")
    .getOrCreate()

  import spark.implicits._

  // Seleccionar los datos en parquet de local
  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load("Users/alvar/Desktop/BD, IA y ML/big-data-processing/final-exercise/solution/src/main/scala/io/keepcoding/spark/exercise/data")
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

  // Cargar datos de postgres
  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://35.225.151.165:5432/postgres")
      .option("dbtable", "metadata")
      .option("user", "postgres")
      .option("password", "keepcoding")
      .load()
  }

  // Join de ambos
  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("antenna")
      .join(
        metadataDF.as("metadata"),
        $"antenna.id" === $"metadata.id"
      ).drop($"metadata.id")
  }

  //Bytes por horas
  override def computeBytesHourly(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes", $"type")
      .groupBy($"type", window($"timestamp", "1 hour"))
      .agg($"id", $"bytes")
      .select($"type", $"window.start".as("timestamp"), $"id", $"bytes")
  }

  //Elegir personas que han superado su cuota
  override def computeUserQuotaLimit(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter($"id" when "type" == "user_total_bytes")
      .select($"timestamp", $"id", $"name", $"quota", $"email")
      .agg(sum($"bytes").as("sum_bytes"))
      .select($"sum_bytes".as("usage"), $"timestamp", $"email", $"quota")
      .when("usage".toInt > "quota".toInt)
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", s"jdbc:postgresql://35.225.151.165:5432/postgres")
      .option("dbtable", "user_quota_limit")
      .option("user", "postgres")
      .option("password", "keepcoding")
      .save()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("Users/alvar/Desktop/BD, IA y ML/big-data-processing/final-exercise/solution/src/main/scala/io/keepcoding/spark/exercise/data/historical")
  }

  def main(args: Array[String]): Unit = run(args)
}
