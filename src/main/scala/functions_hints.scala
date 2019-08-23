import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder
                        .master("local")
                        .appName("My Functions App")
                        .config("spark.driver.memory", "2g")
                        .getOrCreate()

import spark.implicits._

val someDF = spark.read.parquet("some-parquet-file")

// More aggregation functions
// first, last - first a last values in a group. Or in the whole row set if no grouping specified
// we can choose the first non-null, or last non-null by:
  first($"Col", ignoreNulls = true)
// It worth ordering rows in this case

// Sort ordering
someDF.sort($"some-field".desc) // from bigger to smaller
  .sort($"some-field".asc) // the default
  .sort($"some-field".asc_nulls_last) // nulls ordered after values
  .sort($"some-field".asc_nulls_first) // the default

// More string functions
   initcap($"StringColumn") // capitalise the string
// lower, upper - lower case or upper case
// ltrim, rtrim, trim
// lpad, rpad
// format_string("fullName", format_string("%s %s", $"firstName", $"lastName"))
// length, split, reverse
// substring, substring_index, $"col".substring
   $"col".startsWith("start") || $"col".endsWith("end")
   repeat($"col", 10)
// ascii, base64, unbase64, decode, encode

// Search substring in string
  locate(substr = "engineer", str = lower($"jobType")) // find substring in string and return the position or 0
  locate(substr = "subst", str = $"str", pos = 0)
  instr(str = $"str", substring ="substr") // the same as `locate` but the arguments are swapped
  $"some-string-expr".contains("substr") // check of the string contains a substring

// regexp_extract, regexp_replace, translate

// One of (IN)
  $"some-val".isin("val1", "val2") // `in` sql function

// Math functions
// cos, acos, sin, asin, tan, atan, degrees, radians
// bin, hex, unhex - numbers to binary, to/from hex
// abs, round, ceil, floor, shiftLeft, shiftRight
// cbrt, exp, factorial, pow, hypot
// log, log10, log2
// + - * / % > >= < <= ===  <=> && ||
// plus, minus, multiply, divide, mod, gt, gte, lt, lte, equalTo, eqNullSafe, and, or

// Datetime functions
// current_date, current_timestamp, unix_timestamp
// date_add, date_sub, add_month
// date_diff, month_between
// dayofmonth, dayofyear, minute, month, quarter
// last_day, next_date
// date_format, from_unixtime, unix_timestamp
// to_date, to_utc_timestamp

// convert string to date
  to_date(unix_timestamp($"SomeDateColumn", "MM/dd/yyyy").cast("timestamp")).as("SomeDateColumn")
  someDF.selectExpr("to_date(CAST(unix_timestamp(SomeDateColumn, 'MM/dd/yyyy') as TIMESTAMP)) AS SomeDateColumn")

// Misc functions
// array, map, struct - constant wrappers
  lit(" ") // make literal from string
// hash, sha, md5 - security
  rand()
  monotonically_increasing_id() // for unique id
// $"col".isNan, $"col".isNull, $"col".isNotNull
// greatest, least - takes several columns
  $"col".like("expr")
  $"col".rlike("regexp") // regexp as an argument
// get_json_object, json_tuple, to_json, from_json
// input_file_name, spark_partition_id, $"col".explain - useful for debug

//User defined functions
val lambda: String => String = _.split(" ").map(_.capitalize).mkString(" ")
val capitalizeWords = spark.udf.register(name ="capitalizeWords",lambda )
val capitalizeWords_2 = udf(lambda)