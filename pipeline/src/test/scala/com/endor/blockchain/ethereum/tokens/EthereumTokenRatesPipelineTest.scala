package com.endor.blockchain.ethereum.tokens

import com.endor._
import com.endor.infra.{BaseComponent, DIConfiguration}
import com.endor.storage.sources._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.Matchers

trait EthereumTokenRatesPipelineTestComponent extends EthereumTokenRatesPipelineComponent with BaseComponent {
  override val diConfiguration: DIConfiguration = DIConfiguration.ALL_IN_MEM
}

class EthereumTokenRatesPipelineTest extends SharedSQLContext with Matchers {
  private def createContainer(): EthereumTokenRatesPipelineTestComponent = new EthereumTokenRatesPipelineTestComponent {
    override implicit def spark: SparkSession = EthereumTokenRatesPipelineTest.this.spark
  }

  ignore("Basic test") {
    val container = createContainer()
    val inputPath = getClass.getResource("/rates/2018-01-29-16-10.json").toString
    val config = EthereumTokenRatesPipelineConfig(
      inputPath,
      getClass.getResource("/metadata/snapshot-1.parquet").toString,
      DataKey[RateRow](CustomerId("my-customer"), DataId("my-dataset"))
    )
    container.dataFrameIOHandler.saveRawData("someData", inputPath)
    container.driver.run(config)
    val result = container.datasetStore.loadParquet(config.output.inbox).collect()
    val expectedResult = spark.read.parquet(getClass.getResource("/rates/2018-01-29-16-10.parquet").toString)
      .as[RateRow].collect()

    result should contain theSameElementsAs expectedResult
  }
}
