import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

val spark = SparkSession.builder
                        .master("local")
                        .appName("My DataFrame App")
                        .config("spark.driver.memory", "2g")
                        .enableHiveSupport
                        .getOrCreate()

import spark.implicits._

spark.read
     .option("mode", "DROPMALFORMED") // ignore malformed rows
     // .option("mode", "FAILFAST") - fail if there are malformed rows
     // .option("mode", "PERMISSIVE") - the default value "_corrupt_record" fields being added to a malformed row
     .json("my-data.json")
     .na.drop("all") //drop rows with all the empty fields
     // .na.drop("all", Seq("ID", "data.Account", "Amount", "Description", "Date")) - drop if all specified fields are null
     // "any" - drop rows that have at least one empty field
     .na.fill("Unknown", Seq("Description")) // fill Description field with the default value if null
     // .na.fill(Map("Description" -> "Unknown"))
     // .na.fill("Unknown") - for all the Strings (also Numeric and Boolean allowed)
     // .na.replace(Seq("Description"), Map("Movies" -> "Entertainment", "Grocery Store" -> "Food"))
     .where($"Amount" =!= 0 || $"Description" === "Unknown") // same as filter
     .write.mode(SaveMode.Overwrite).parquet("some-parquet-file")

val usersDF = spark.createDataFrame(List(("JustinPihony", 151), ("SampleUser", 73)))
val namedDF = usersDF.toDF("UserName", "Score")

val companiesDF = spark.sql("select * from parquet.`some-parquet-file`")

companiesDF.createOrReplaceTempView("Companies")

val copiedSession = spark.newSession() // multiple independent sessions
SparkSession.setActiveSession(copiedSession) // switching

// Actions:
// - count
// - first/head/take/takeAsList

// Transformations:
// - except/intersect/union
// - sort/sortWithPartitions/orderBy
// - sample/randomSplit

// Operations:
// - map/mapPartitions/flatMap/foreach/foreachPartition

// checkpoint - for resilient caching on DF

companiesDF
  .select($"col1", $"col2")
  .distinct()
  //.dropDuplicates()
  //.dropDuplicates("col1", "col2") - for compound key
  .coalesce(5) // change the number of partitions
  .toJSON // convert each row to a json string

companiesDF
  .select($"col1", $"col2.some_field" as "some_field")
  .groupBy($"col1")
  .agg(avg($"some_field"), min($"some_field"), max($"some_field"),
       sum($"some_field"), sumDistinct($"some_field"),
       count($"some_field"), countDistinct($"some_field"),
       stddev_samp($"some_field"), stddev_pop($"some_field"),
       collect_set($"employee").as("UniqueEmployees"),
       collect_list($"employee").as("employees"))

// Flatten inner array from row
val employeeStructDF = companiesDF.select($"company", explode($"employees").as("employee"))
// explode_outer - preserves nulls
// posexplode - add an additional row that specifies position in the array

// Extract struct fields
val employeesDF = employeeStructDF.select($"company", expr("employee.firstName as firstName"))

// case when then else end
employeesDF.select($"*", when($"company" === "FamilyCo", "Premium").when($"company" === "OldCo", "Legacy").otherwise("Standard")).show

// convert string to date
to_date(unix_timestamp($"SomeDateColumn", "MM/dd/yyyy").cast("timestamp")).as("SomeDateColumn")
//.selectExpr("to_date(CAST(unix_timestamp(SomeDateColumn, 'MM/dd/yyyy') as TIMESTAMP)) AS SomeDateColumn")



// Save to Cassandra
import org.apache.spark.sql.cassandra._
employeesDF
  .write
  .mode(SaveMode.Overwrite)
  .cassandraFormat("account_aggregates", "finances")
  .save