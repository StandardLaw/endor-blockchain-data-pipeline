package com.endor.blockchain.ethereum.tokens

package object ratesaggregation {
  val open: OpenRateAggregator = new OpenRateAggregator()
  val close: CloseRateAggregator = new CloseRateAggregator()
}
