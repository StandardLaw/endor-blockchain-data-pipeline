package com.endor.blockchain.ethereum.transaction

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration}
import com.endor.infra.{BaseComponent, DIConfiguration}
import com.endor.jobnik.JobnikSession
import org.apache.spark.sql.SparkSession

object EMRTransactionsPipeline extends SparkApplication[EthereumTransactionsPipelineConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[EthereumTransactionsPipelineConfig]): EntryPointConfig =
    EntryPointConfig("EthereumBlocksToTransactions")


  override protected def run(sparkSession: SparkSession, diConf: DIConfiguration,
                             configuration: EthereumTransactionsPipelineConfig)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val container = new EthereumTransactionsPipelineComponent with BaseComponent {
      override implicit def spark: SparkSession = sparkSession
      override val diConfiguration: DIConfiguration = diConf
    }
    container.driver.run(configuration)
  }
}