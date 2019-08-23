import data.{Transaction, TransactionForAvg}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

val spark = SparkSession.builder()
  .master("local")
  .appName("My Dataset App")
  .config("spark.driver.memory", "2g")
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._

val financesDS = spark.read
  .option("wholeFile", "true") // read the whole json file
  .option("samplingRatio", "(0..1.0]") // only a portion of json should be read to infer a schema
  .json("finances.json")
  .withColumn("date", to_date(unix_timestamp($"Date", "MM/dd/yyyy")).cast("timestamp"))
  .as[Transaction] // this converts DataFrame to a Dataset[data.Transaction]

  // Make some NA fields processing
  .na.drop("all", Seq("ID", "data.Account", "Amount", "Description", "Date"))
   // `na` method converts Dataset back to a DataFrame
  .na.fill("Unknown", Seq("Description"))
  .as[Transaction] // make it a Dataset again
  // there is also a `filter` method that takes a lambda, but it's like a black box for an optimizer
  .where($"Amount" =!= 0 || $"Descriptions" === "Unknown")
  // `Amount` should be null here as otherwise a runtime exception will be thrown

  // Transform the dataset
  // .transform { txDS: Dataset[Transaction] => someOtherDS}

// Aggregation
financesDS.select(
  $"data.Account.Number".as("AccountNumber").as[String],
  $"Amount".as[Double],
  $"Date".as[java.sql.Date](Encoders.DATE), // encoder can be passed explicitly
  $"Description".as[String]
).as[TransactionForAvg]
  .groupByKey(_.accountNumber)

  // .keys - Dataset of keys
  // .count
  // .cogroup(otherGrouping)(combinerFunction)
  // .keyAs[KeyType] - specify type of a key

  .agg(
    typed.avg[TransactionForAvg](_.amount).as("AverageTransaction").as[Double],
    typed.sum[TransactionForAvg](_.amount),
    // there is also typed.sumLong
    typed.count[TransactionForAvg](_.amount),
    max($"Amount").as("MaxTransaction").as[Double] // untyped aggregation with further cast
  )

// another way for aggregation instead of `agg` method. Uses a black box lambda which is bad for an optimizer:y
// .mapGroups((key, txs) => (key, txs.map(_.amount).sum)) // no shuffle here
// .flatMapGroups // also no shuffle here
// .reduceGroups // also no shuffle here
// .mapValues // map values without reduction. No partition shuffling here as well

//Save to Cassandra. Spark Cassandra connector should be in a classpath for that
  .write
  .mode(SaveMode.Overwrite)
  //.format("org.apache.spark.sql.cassandra")
  //.options(Map("keyspace" -> "finances", "table" -> "account_aggregates")) //same as bellow
  .cassandraFormat("finances", "account_aggregates")
  .save
