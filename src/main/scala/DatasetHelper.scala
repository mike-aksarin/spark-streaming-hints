import org.apache.spark.sql.Dataset

import scala.util.Try

object DatasetHelper {

  implicit class DS[T](ds: Dataset[T]) {

    // Hides AnalysisException
    def hasColumn(columnName: String): Boolean =  Try(ds(columnName)).isSuccess
  }

}
