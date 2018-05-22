package com.endor.blockchain.ethereum.transaction

import com.endor._
import com.endor.infra.spark.SparkDriverSuite
import com.endor.infra.{BaseComponent, DIConfiguration}
import com.endor.storage.sources._
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

trait EthereumTransactionsPipelineTestComponent extends EthereumTransactionsPipelineComponent with BaseComponent {
  override val diConfiguration: DIConfiguration = DIConfiguration.ALL_IN_MEM
}


class EthereumTransactionsPipelineTest extends FunSuite with SparkDriverSuite {
  private def createContainer(): EthereumTransactionsPipelineTestComponent =
    new EthereumTransactionsPipelineTestComponent {
      override implicit def spark: SparkSession = EthereumTransactionsPipelineTest.this.spark
    }

  test("Parsing block 5203600") {
    val input_path = getClass.getResource("/com/endor/blockchain/ethereum/blocks/raw/5203600.bin").toString
    val expected_path = getClass.getResource("/com/endor/blockchain/ethereum/blocks/parsed/5203600.parquet").toString
    val container = createContainer()

    val config = EthereumTransactionsPipelineConfig(input_path, DataKey(CustomerId("test_customerID"), DataId("test_dataID")))
    container.driver.run(config)
    val results = container.datasetStore.loadParquet(config.output.inbox).collect()
    val expected = spark.read.parquet(expected_path).as[ProcessedTransaction].collect()
    results should contain theSameElementsAs expected
  }
}