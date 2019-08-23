import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._


val spark = SparkSession.builder()
  .master("local[1]")
  .appName("My Streaming App")
  .config("spark.driver.memory", "2g")
  .enableHiveSupport()
  .getOrCreate()

val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
val rddStreams = (1 to 10).map(x => spark.sparkContext.makeRDD(List(x % 4)))
val queue = scala.collection.mutable.Queue(rddStreams: _*)

// Create a DStream
val testStream = ssc.queueStream(queue, oneAtATime = true) // single element batches

testStream.print

// Or:
testStream.count.print // by default is a number of items in a batch

// Or:
testStream.countByValue().print // returns Map(value -> count)

ssc.remember(Seconds(60)) // last 60 seconds of data in a batch

val currMillis = System.currentTimeMillis
val startTime = Time(currMillis - (currMillis % 5000))

ssc.start

// wait for some batches to read

// Get a slice from these batches
val slicedRDDs = testStream.slice(startTime, startTime + Seconds(10))
slicedRDDs.foreach(rdd => rdd.foreach(println))

ssc.stop(stopSparkContext = false)

// More methods
testStream
//  .map, .mapPartitions, .flatMap
//  .filter
//  .reduce
//  .glom - coalesces all elements within each partition into an array
//  .repartition
//  .context
//  .cache(), .persist() - some stateful methods call `persist` automatically
//  .saveAsObjectFiles(),  .saveAsTextFiles()

ssc
//  .union - unified DStream from multiple DStreams
//  .transform() - apply a function on RDD

// PairDStream
//  .mapValues, .flatMapValues
//  .groupByKey, .reduceByKey, .combineByKey
//  .cogroup
//  .join, .fullOuterJoin, .leftOuterJoin, .rightOuterJoin
