package com.endor.blockchain.ethereum.blocksummaries

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration}
import com.endor.infra.{BaseComponent, DIConfiguration, LoggingComponent}
import com.endor.jobnik.JobnikSession
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext

object EMRBlockSummaryPipeline extends SparkApplication[BlockSummaryPipelineConfiguration] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[BlockSummaryPipelineConfiguration]): EntryPointConfig =
    EntryPointConfig("ParseBlockSummaries")


  override protected def run(sparkSession: SparkSession, diConf: DIConfiguration,
                             configuration: BlockSummaryPipelineConfiguration)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val container: BlockSummaryPipelineComponent with BaseComponent with LoggingComponent =
      new BlockSummaryPipelineComponent with BaseComponent with LoggingComponent {
        override implicit def spark: SparkSession = sparkSession
        override val diConfiguration: DIConfiguration = diConf
      }
    implicit val ec: ExecutionContext = ExecutionContext.global
    container.driver.run(configuration)
  }
}