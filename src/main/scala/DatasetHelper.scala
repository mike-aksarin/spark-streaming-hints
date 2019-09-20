import org.apache.spark.sql.Dataset
import scala.util.Try

object DatasetHelper {

  implicit class DS[T](ds: Dataset[T]) {

    // Hides AnalysisException
    def hasColumn(columnName: String): Boolean =  Try(ds(columnName)).isSuccess

    def corrupted: Option[Dataset[String]] = {
      if (hasColumn("_corrupt_record")) {
        import ds.sparkSession.implicits._
        val corrupted = ds
          .where($"_corrupt_record".isNotNull)
          .select($"_corrupt_record")
          .as[String]
        Some(corrupted)
      } else {
        None
      }
    }
  }

}
