package data

case class EvaluatedSimpleTransaction(
  tx: SimpleTransaction,
  isPossibleFraud: Boolean
)
