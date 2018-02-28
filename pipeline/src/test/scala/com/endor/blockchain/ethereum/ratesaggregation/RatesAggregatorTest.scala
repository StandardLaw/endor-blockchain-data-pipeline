package com.endor.blockchain.ethereum.ratesaggregation

import java.sql.Timestamp

import com.endor.blockchain.ethereum.tokens.RateRow
import com.endor.infra.spark.SparkDriverFunSuite

class RatesAggregatorTest extends SparkDriverFunSuite {
  private def generateRandomRow(name: String, symbol: String, address: String): RateRow =
    RateRow(name, symbol, randomGenerator.nextDouble(), Option(name), Option(symbol), Option(address),
      randomDate(Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-01 23:59:59")))

  test("Open rate aggregator") {
    val sess = spark
    import sess.implicits._
    val data = (0 to 10).map(_ => generateRandomRow("myToken", "MT", "0x1337"))
    val ds = spark.createDataset(data)
    val expectedOpenRate = data.minBy(_.timestamp.getTime).price
    val openRate = ds.agg(open($"timestamp", $"price")).as[Double].head()
    openRate should equal(expectedOpenRate)
  }

  test("Open rate aggregator multiple tokens") {
    val sess = spark
    import sess.implicits._
    val data = (0 to 10).flatMap(_ => {
      val token = randomString(5)
      val symbol = randomString(5)
      val address = randomString(5)
      (0 to randomGenerator.nextInt(15)).map(_ => generateRandomRow(token, symbol, address))
    })
    val ds = spark.createDataset(data)
    val expectedOpenRates = data.groupBy(_.rateName).mapValues(_.minBy(_.timestamp.getTime).price)
    val openRates = ds.groupBy("rateName").agg(open($"timestamp", $"price") as "open")
      .as[(String, Double)].collect().toMap
    openRates should contain theSameElementsAs expectedOpenRates
  }

  test("Close rate aggregator") {
    val sess = spark
    import sess.implicits._
    val data = (0 to 10).map(_ => generateRandomRow("myToken", "MT", "0x1337"))
    val ds = spark.createDataset(data)
    val expectedCloseRate = data.maxBy(_.timestamp.getTime).price
    val closeRate = ds.agg(close($"timestamp", $"price")).as[Double].head()
    closeRate should equal(expectedCloseRate)
  }

  test("Close rate aggregator multiple tokens") {
    val sess = spark
    import sess.implicits._
    val data = (0 to 10).flatMap(_ => {
      val token = randomString(5)
      val symbol = randomString(5)
      val address = randomString(5)
      (0 to randomGenerator.nextInt(15)).map(_ => generateRandomRow(token, symbol, address))
    })
    val ds = spark.createDataset(data)
    val expectedCloseRates = data.groupBy(_.rateName).mapValues(_.maxBy(_.timestamp.getTime).price)
    val closeRates = ds.groupBy("rateName").agg(close($"timestamp", $"price") as "open")
      .as[(String, Double)].collect().toMap
    closeRates should contain theSameElementsAs expectedCloseRates
  }
}
