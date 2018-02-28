package com.endor.blockchain.ethereum.rates

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration}
import com.endor.infra.{BaseComponent, DIConfiguration}
import com.endor.jobnik.JobnikSession
import org.apache.spark.sql.SparkSession

object EMREthereumRates extends SparkApplication[EthereumRatesPipelineConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[EthereumRatesPipelineConfig]): EntryPointConfig =
    EntryPointConfig("EtheruemTokenRates")


  override protected def run(sparkSession: SparkSession, diConf: DIConfiguration,
                             configuration: EthereumRatesPipelineConfig)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val container = new EthereumRatesPipelineComponent with BaseComponent {
      override val diConfiguration: DIConfiguration = diConf
      override implicit def spark: SparkSession = sparkSession
    }
    container.driver.run(configuration)
  }
}
