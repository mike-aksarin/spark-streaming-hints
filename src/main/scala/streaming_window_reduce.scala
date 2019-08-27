import data.{AggregateData, EvaluatedSimpleTransaction, SimpleTransaction}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/* Window
 *
 * Number of items in a window. Must be less than a window duration
 * batchSize (say, 1 sec)
 *
 * Period of time between executions of the window logic.
 * Time between the prev window start and the next window start
 * slideInterval (say, 2 seconds)
 *
 * If window duration is greater than a slide, then same batches will occur in an overlapping windows
 * windowLength (say, 3 seconds)
 */

// Checkpoints are required for stateful streaming, i.e. when using window
// Local dir could be used only for development, in production some reliable storage needed, like HDFS
val checkpointDir = "file:///checkpoint"

val storedHistoricDataName = "storedHistoric"


val spark = SparkSession.builder()
  .master("local[3]") // more executors as we need some for a kafka connector
  .appName("Stateful Streaming App")
  .config("spark.driver.memory", "2g")
  .enableHiveSupport()
  .getOrCreate()

val zkHost = "localhost:2181"
val kafkaGroupId = "transaction-group"
val kafkaTopics = Map("transaction" -> 1) //number of threads for a topic consumer


// To do actual checkpoint recovery you can run a cluster with a `--supervise` flag:
// spark-submit --deploy-mode cluster --supervise
// In a Yarn: `yarn.resourcemanager.am.max-attempts` property
val streamingContext = StreamingContext.getOrCreate(checkpointDir, createContext)
// Use `createOnError = true` parameter of  `StreamingContext.getOrCreate` to enable spark throw an exception,
// if `checkpointDir` exists but could not be restored possibly due to logic change

def createContext(): StreamingContext = {
  val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
  // If you omit this, then spark will not complain, but checkpoint logic will not work
  ssc.checkpoint(checkpointDir)

  val kafkaStream = KafkaUtils.createStream(ssc, zkHost, kafkaGroupId, kafkaTopics)

  // To prevent data loss in a case of failure:
  // val brokerHostList = "localhost:9092"
  // val settings = Map("metadata.broker.list" -> brokerHostList, "group.id" -> kafkaGroupId)
  // val kafkaTopicSet = kafkaTopics.keySet
  // val kafkaStream = KafkaUtils.createDirectStream(streamingContext, settings, kafkaTopicSet)

  kafkaStream
    // Checkpoint interval could be configured like this:
    // (You can start with frequencyInterval = 5-10 times of sliding interval)
    //.checkpoint(frequencyInterval)
    .map { case (key, value) => SimpleTransaction.parseLine(value) }
    .flatMap (_.right.toOption)
    .map { tx => (tx.accountNumber -> tx)} // add a key to access PairDStream methods
    .mapValues(Seq(_))
    // `reduceByKeyAndWindow` with `invReduceFunc` parameter boosts the performance in comparison with ordinary `window` call
    .reduceByKeyAndWindow(
      reduceFunc = (txs: Seq[SimpleTransaction], other: Seq[SimpleTransaction]) => txs ++ other,
      invReduceFunc = (txs: Seq[SimpleTransaction], old: Seq[SimpleTransaction]) => txs diff old,
      windowDuration = Seconds(10),
      slideDuration = Seconds(10)
    )
    // `mapWithState` could be 10 times more efficient than `updateStateByKey`
    .mapWithState(stateSpecObject)
    .stateSnapshots()
    // Some more transforming, filtering and output logic here
  ssc
}

def stateSpecObject: StateSpec[String, Seq[SimpleTransaction], AggregateData, Seq[EvaluatedSimpleTransaction]] = {
  StateSpec.function(aggregateWindowData _)
  // There are more StateSpec methods:
  //    .numPartitions(amount)
  //    .partitioner(customPartitioner)
  //    .initialState(someKeyValueRdd)
  //    .timeout(Seconds(30)) // idle duration after that the state should be dropped
}

def aggregateWindowData(acctNum: String, newTxsOpt: Option[Seq[SimpleTransaction]],
                        state: State[AggregateData]): Seq[EvaluatedSimpleTransaction] = {
  val newTxs = newTxsOpt.getOrElse(List.empty)
  val (windowTotal, windowNumTxs) = newTxs.foldLeft(0.0 -> 0) { (currAgg, tx) =>
    val (totalSpending, numTxs) = currAgg
    (totalSpending + tx.amount) -> (numTxs + 1)
  }
  val windowAvg = if (windowNumTxs > 0) windowTotal / windowNumTxs else 0
  val calculatedAgg = AggregateData(windowTotal, windowNumTxs, windowAvg)

  val curDataOpt = state.getOption()

  val newData = curDataOpt.fold(calculatedAgg)(_.aggregate(calculatedAgg))
  state.update(newData)
  newTxs.map(tx => EvaluatedSimpleTransaction(tx, tx.amount > 4000))
}

streamingContext.start()
streamingContext.awaitTermination()

// statistic histograms could be viewed at localhost:4040/streaming/