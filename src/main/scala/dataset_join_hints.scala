import data.{Person, Role}
import org.apache.spark.sql.{Dataset, SparkSession}

val spark = SparkSession.builder()
  .master("local")
  .appName("Dataset Joins App")
  .config("spark.driver.memory", "2g")
  .enableHiveSupport()
  .getOrCreate()

import spark.implicits._

val personDS = List(Person(1, "Justin", "Pihony"), Person(2, "John", "Doe")).toDS
val roleDS = List(Role(1, "Manager"), Role(3, "Huh")).toDS

val innerJoinDS: Dataset[(Person, Role)] =
  personDS.joinWith(roleDS, personDS("id") === roleDS("id"), joinType = "inner")
  // other join types are also available, but `null` columns should be mapped correctly
