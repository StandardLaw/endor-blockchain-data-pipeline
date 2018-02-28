package com.endor.blockchain.ethereum

package object ratesaggregation {
  val open: OpenRateAggregator = new OpenRateAggregator()
  val close: CloseRateAggregator = new CloseRateAggregator()
}
