import com.datastax.spark.connector.streaming._
import data.SimpleTransaction
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

val spark = SparkSession.builder()
  .master("local[3]") // more executors as we need some for a kafka connector
  .appName("My Streaming App")
  .config("spark.driver.memory", "2g")
  .enableHiveSupport()
  .getOrCreate()

val streamingContext = new StreamingContext(spark.sparkContext, Seconds(1))
val zkHost = "localhost:2181"
val kafkaGroupId = "transaction-group"
val kafkaTopics = Map("transaction" -> 1) //number of threads for a topic consumer

// Create a DStream
val kafkaStream = KafkaUtils.createStream(streamingContext, zkHost, kafkaGroupId, kafkaTopics)

// To prevent data loss in a case of failure:
// val brokerHostList = "localhost:9092"
// val settings = Map("metadata.broker.list" -> brokerHostList, "group.id" -> kafkaGroupId)
// val kafkaTopicSet = kafkaTopics.keySet
// val kafkaStream = KafkaUtils.createDirectStream(streamingContext, settings, kafkaTopicSet)

// kafkaStream.print // output stream to console

val cassandraKeyspace = "finances"
val cassandraTableName = "transactions"

kafkaStream
  .map { case (key, value) => SimpleTransaction.parseLine(value) }
  .flatMap (_.right.toOption)

  // save via RDD function. RDD could be empty in foreachRDD method!
  //.foreachRDD(tx => tx.saveToCassandra(cassandraKeyspace, cassandraTableName))

  // save via connector DStream function
  .saveToCassandra(cassandraKeyspace, cassandraTableName)

// Above needs a Cassandra table to be created. Here is CQL for that:
// CREATE TABLE finances.transactions(id bigint, account_number text, amount double,
//                                    date date, description text, PRIMARY KEY (ID));

streamingContext.start //asynchronous
streamingContext.awaitTermination //blocking
// streamingContext.awaitTerminationOrTimeout(5000)

// There is also a streamingContext.stop method
