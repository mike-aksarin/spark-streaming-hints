import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.functions._

// To have Structured Streaming instead of batches:

val spark = SparkSession.builder
  .master("local[3]")
  .appName("Fraud Detector")
  .config("spark.driver.memory", "2g")
  .config("spark.cassandra.connection.host", "localhost")
  .enableHiveSupport
  .getOrCreate()

import spark.implicits._

// Open a stream instead of reading batches

val someDF = spark
  .readStream // instead of `read`!
  .format(source = "socket") // or other
  .option("host", "localhost")
  .option("port", "9999")
  .option("includeTimestamp", true) // adds "timestamp" column
  .load // !

// Use a watermark to reduce the state
// Use also a window
val timeColumn = $"timestamp"
val timeColumnName = "timestamp"
val valueColumn = $"value"

val countDF = someDF
  .withWatermark(timeColumnName, delayThreshold = "10 seconds")
  .groupBy(window(timeColumn, windowDuration = "5 seconds", slideDuration = "5 seconds"), valueColumn)
  .count()

// Save a stream

val query = countDF
  .writeStream // instead of `write`!
  .format(source = "console") // or other
  .option("truncate", false)
  .trigger(ProcessingTime("5 seconds")) // or other condition, optional
  .outputMode("append") // "update" and "complete" are also available
  .start // !

// Monitoring structured streams:

//spark.streams.

query.exception

query.id
query.runId

query.sparkSession

query.stop

query.status

query.lastProgress