package com.endor.blockchain.ethereum.tokens

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration, SparkInfrastructure}
import com.endor.infra.{LoggingComponent, SparkSessionComponent}
import com.endor.jobnik.JobnikSession
import com.endor.storage.io.{IOHandler, IOHandlerComponent, S3IOHandler}
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext

object EMRTokensPipeline extends SparkApplication[EthereumTokensPipelineConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[EthereumTokensPipelineConfig]): EntryPointConfig =
    EntryPointConfig("EthereumBlocksToTokenTransactions")


  override protected def run(spark: SparkSession, configuration: EthereumTokensPipelineConfig)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val component = new EthereumTokenPipelineComponent with SparkSessionComponent
      with LoggingComponent with IOHandlerComponent {
      override implicit def sparkSession: SparkSession = spark

      override implicit def ioHandler: IOHandler = new S3IOHandler(SparkInfrastructure.EMR(true))
    }
    implicit val ec: ExecutionContext = ExecutionContext.global
    component.driver.run(configuration)
  }
}