package com.endor.blockchain.ethereum.block

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration, SparkInfrastructure}
import com.endor.jobnik.JobnikSession
import com.endor.storage.io.S3IOHandler
import org.apache.spark.sql.SparkSession

object EMRBlocksPipeline extends SparkApplication[EthereumBlocksPipelineConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[EthereumBlocksPipelineConfig]): EntryPointConfig =
    EntryPointConfig("EthereumBlocksToTransactions")


  override protected def run(sparkSession: SparkSession, configuration: EthereumBlocksPipelineConfig)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val ioHandler = new S3IOHandler(SparkInfrastructure.EMR(true))
    val driver = new EthereumBlocksPipeline(ioHandler)(sparkSession)
    driver.run(configuration)
  }
}