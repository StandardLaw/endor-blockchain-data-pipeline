package com.endor.blockchain.ethereum.tokens

import com.endor.storage.io.InMemoryIOHandler
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.Matchers

class EthereumTokenRatesPipelineTest extends SharedSQLContext with Matchers {
  test("Basic test") {
    val ioHandler = new InMemoryIOHandler()
    val inputPath = getClass.getResource("/rates/2018-01-29-16-10.json").toString
    val config = EthereumTokenRatesPipelineConfig(
      inputPath,
      getClass.getResource("/metadata/snapshot-1.parquet").toString,
      ""
    )
    ioHandler.saveRawData("someData", inputPath)
    val pipeline = new EthereumTokenRatesPipeline(ioHandler)
    val result = pipeline.process(config).collect()
    val expectedResult = spark.read.parquet(getClass.getResource("/rates/2018-01-29-16-10.parquet").toString)
      .as[RateRow].collect()

    result should contain theSameElementsAs expectedResult
  }
}
