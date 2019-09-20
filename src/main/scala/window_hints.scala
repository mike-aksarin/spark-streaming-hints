import data.Status
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val spark: SparkSession = SparkSession.builder
  .master("local")
  .appName("My Window App")
  .config("spark.driver.memory", "2g")
  .getOrCreate()

import spark.implicits._

val statuses = spark.createDataFrame(List(Status(1, "Justin", "New"), Status(2, "Justin", "Open")))

val financesDF = spark.read.parquet("some-parquet-file")

// Windows
val windowSpec = Window
  .partitionBy($"AccountNumber")
  .orderBy($"Date")
  .rowsBetween(-4, 0)
  //.rangBetween - slide by column value instead of row number.
  // The rangBetween expr should be a single numeric expr. It could be multiple or / and String if the offset is 0
  // Window.unboundedFollowing, Window.unboundedPreceding, Window.currentRow are also available as bounds

val rollingAvg = avg($"Amount").over(windowSpec)

// Special window functions
val win = Window.orderBy($"id").partitionBy($"customer")
statuses.select($"id", $"customer", $"status", lag($"status", 1).over(win) as "prevStatus")
// lag stands for previous row with a given offset (how many rows back we want to go)
// lag(colName, offset, defaultValue) is also available.
// defaultValue is used when there are no rows for a given offset to read from. Say for the first row

row_number() // the position of the row in each separate window

rank() // same numbers for the tied ordering when windowing field values are the same, still keeping track of the total rows encountered
// Say [tie, rank = 1], [tie, rank = 1], [next, rank = 3]

dense_rank() // doesn't track the total rows for tied ordering.
// Say [tie, dense_rank = 1], [tie, dense_rank = 1], [next, dense_rank = 2]

percent_rank() // rank divided by the number of rows

// window by time function for streaming
val financeDFWindowed = financesDF.select($"*", window(timeColumn = $"Date",
                                                             windowDuration = "30 days",
                                                             slideDuration = "30 days",
                                                             startTime = "15 minutes"))