package data

case class AggregateData(
  totalSpending: Double,
  numTx: Int,
  windowSpendingAvg: Double
 ) {

  def averageTx: Double = {
    if (numTx > 0) totalSpending / numTx else 0
  }

  def aggregate(newWindowData: AggregateData): AggregateData = {
    AggregateData(
     totalSpending + newWindowData.totalSpending,
     numTx + newWindowData.numTx,
     newWindowData.windowSpendingAvg
    )
  }

}

object AggregateData {


}