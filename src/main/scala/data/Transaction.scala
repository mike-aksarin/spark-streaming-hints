package data

case class Transaction(
  id: Long,
  account: Account,
  date: java.sql.Date,
  amount: Double,
  description: String
)
