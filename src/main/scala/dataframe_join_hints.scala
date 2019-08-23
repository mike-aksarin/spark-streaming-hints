import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
  .master("local")
  .appName("My Join App")
  .config("spark.driver.memory", "2g")
  .getOrCreate()

val personDF = spark.read.parquet("person")
val roleDF = spark.read.parquet("role")

personDF
  //.join(roleDF, $"col1" === $"col2") // for the column name collision
  .join(roleDF, personDF("id") === roleDF("id"), joinType = "inner") // for the column name collision
  .join(roleDF, usingColumn = "id")
  .join(roleDF, usingColumns = Seq("id"), joinType = "left")
  // "right" = "right_outer"
  // "full" = "outer" = "full_outer"
  // "left_semi" - don't add right table columns, but remove rows that don't have corresponding rows in a right table
  // "left_anti" - opposite to "left_semi", keep only the rows that don't have corresponding right side

personDF.crossJoin(roleDF) // Cross join (Cartesian join) - every cross row combinations
  // joinType = "cross" - the same

  // also udf can be used for joins, but the give n*n complexity for Cartesian join
  // instead of n*log n for SortMergeJoin