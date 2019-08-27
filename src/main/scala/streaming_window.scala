import data.{AggregateData, SimpleTransaction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
    .window(windowDuration = Seconds(10), slideDuration = Seconds(10))
    .updateStateByKey[AggregateData](aggregateWindowData)
    .transform(joinHistoricData _)
    .filter { case (acctNum, (aggData, historicAvg)) => isRedFlag(acctNum, aggData, historicAvg) }
    .print
  ssc
}

/** Function to be called via `PairDStream.updateStateByKey` method for each window of transaction stream
  * @param newTxs New data portion, could be empty if no values arrived
  * @param currentState Previously saved state, could be `None` if no state has been saved already
  * @return the new state, to delete the state just return `None`
  * existing state & no data  => (Seq.empty, Option(state))
  * no state       & has data => (Seq(...), None)
  * existing state & has data => (Seq(...), Option(state))
  */
def aggregateWindowData(newTxs: Seq[SimpleTransaction], currentState: Option[AggregateData]): Option[AggregateData] = {
  val (windowTotal, windowNumTxs) = newTxs.foldLeft(0.0 -> 0) { (currAgg, tx) =>
    val (totalSpending, numTxs) = currAgg
    (totalSpending + tx.amount) -> (numTxs + 1)
  }
  val windowAvg = if (windowNumTxs > 0) windowTotal / windowNumTxs else 0
  val calculatedAgg = AggregateData(windowTotal, windowNumTxs, windowAvg)
  val newState = currentState.fold(calculatedAgg)(_.aggregate(calculatedAgg))
  Option(newState)
}

def joinHistoricData(dStreamRdd: RDD[(String, AggregateData)]): RDD[(String, (AggregateData, Double))] = {
  val sc = dStreamRdd.sparkContext
  val persistentRdds = sc.getPersistentRDDs.values
  val historicRddOpt = persistentRdds.find(_.name == storedHistoricDataName)
  val historicRdd = historicRddOpt.fold(retrieveHistoricData(sc))(_.asInstanceOf[RDD[(String, Double)]])
  dStreamRdd.join(historicRdd)
}

def retrieveHistoricData(sc: SparkContext): RDD[(String, Double)] = {
  import com.datastax.spark.connector._
  val historicRdd = sc
    .cassandraTable("finances", "account_aggregate")
    .select("account_number", "average_transaction")
    .map(row => row.get[String]("account_number") -> row.get[Double]("average_transaction"))
  historicRdd.setName(storedHistoricDataName)
  historicRdd.cache
}

def isRedFlag(accountNum: String, aggregateData: AggregateData, historicAvg: Double): Boolean = {
  aggregateData.averageTx - historicAvg > 2000 || aggregateData.windowSpendingAvg - historicAvg > 2000
}

streamingContext.start()
streamingContext.awaitTermination()

// statistic histograms could be viewed at localhost:4040/streaming/