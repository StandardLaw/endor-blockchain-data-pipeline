package com.endor.blockchain.ethereum.transaction

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration, SparkInfrastructure}
import com.endor.jobnik.JobnikSession
import com.endor.storage.io.S3IOHandler
import org.apache.spark.sql.SparkSession

object EMRTransactionsPipeline extends SparkApplication[EthereumTransactionsPipelineConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[EthereumTransactionsPipelineConfig]): EntryPointConfig =
    EntryPointConfig("EthereumBlocksToTransactions")


  override protected def run(sparkSession: SparkSession, configuration: EthereumTransactionsPipelineConfig)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val ioHandler = new S3IOHandler(SparkInfrastructure.EMR(true))
    val driver = new EthereumTransactionsPipeline(ioHandler)(sparkSession)
    driver.run(configuration)
  }
}