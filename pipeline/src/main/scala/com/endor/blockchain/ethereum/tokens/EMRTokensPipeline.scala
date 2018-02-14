package com.endor.blockchain.ethereum.tokens

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra._
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration}
import com.endor.jobnik.JobnikSession
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext

object EMRTokensPipeline extends SparkApplication[EthereumTokensPipelineConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[EthereumTokensPipelineConfig]): EntryPointConfig =
    EntryPointConfig("EthereumBlocksToTokenTransactions")


  override protected def run(sparkSession: SparkSession, dIConf: DIConfiguration, configuration: EthereumTokensPipelineConfig)
                            (implicit endorContext: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val component = new EthereumTokenPipelineComponent with BaseComponent with LoggingComponent {
      override implicit def spark: SparkSession = sparkSession
      override val diConfiguration: DIConfiguration = dIConf
    }
    implicit val ec: ExecutionContext = ExecutionContext.global
    component.driver.run(configuration)
  }
}