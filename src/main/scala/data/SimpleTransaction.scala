package data

import java.text.SimpleDateFormat

import scala.util.control.NonFatal

case class SimpleTransaction(
  id: Long,
  accountNumber: String,
  date: java.sql.Date,
  amount: Double,
  description: String
)

object SimpleTransaction {

  def parseLine(line: String): Either[MalformedLine, SimpleTransaction] = {
    line.split(",") match {
      case Array(idStr, dateStr, acctNum, amt, desc) =>
        try {
          Right(SimpleTransaction(
            idStr.toLong,
            acctNum,
            new java.sql.Date(new SimpleDateFormat("MM/dd/yyyy").parse(dateStr).getTime),
            amt.toDouble,
            desc
          ))
        } catch {
          case NonFatal(e) => Left(MalformedLine(line, Some(e)))
        }
      case _ => Left(MalformedLine(line))
    }
  }
}
