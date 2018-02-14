package com.endor.blockchain.ethereum.tokens

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration}
import com.endor.infra.{BaseComponent, DIConfiguration}
import com.endor.jobnik.JobnikSession
import org.apache.spark.sql.SparkSession

object EMREthereumTokenRates extends SparkApplication[EthereumTokenRatesPipelineConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[EthereumTokenRatesPipelineConfig]): EntryPointConfig =
    EntryPointConfig("EtheruemTokenRates")


  override protected def run(sparkSession: SparkSession, diConf: DIConfiguration,
                             configuration: EthereumTokenRatesPipelineConfig)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val container = new EthereumTokenRatesPipelineComponent with BaseComponent {
      override val diConfiguration: DIConfiguration = diConf
      override implicit def spark: SparkSession = sparkSession
      override def tokenListScraper: EthereumTokensOps.TokenListScraper = EthereumTokensOps.coinMarketCapScraper
    }
    container.driver.run(configuration)
  }
}
