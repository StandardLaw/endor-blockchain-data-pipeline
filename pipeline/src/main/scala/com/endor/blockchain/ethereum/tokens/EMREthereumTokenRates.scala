package com.endor.blockchain.ethereum.tokens

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration, SparkInfrastructure}
import com.endor.jobnik.JobnikSession
import com.endor.storage.io.S3IOHandler
import org.apache.spark.sql.SparkSession

object EMREthereumTokenRates extends SparkApplication[EthereumTokenRatesPipelineConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[EthereumTokenRatesPipelineConfig]): EntryPointConfig =
    EntryPointConfig("EthereumBlocksToTokenTransactions")


  override protected def run(sparkSession: SparkSession, configuration: EthereumTokenRatesPipelineConfig)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val ioHandler = new S3IOHandler(SparkInfrastructure.EMR(true))
    val driver = new EthereumTokenRatesPipeline(ioHandler)(sparkSession)
    driver.run(configuration)
  }
}
