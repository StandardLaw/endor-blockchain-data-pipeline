package com.endor.blockchain.ethereum.tokens

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.{BaseComponent, DIConfiguration}
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration}
import com.endor.jobnik.JobnikSession
import org.apache.spark.sql.SparkSession

object EMRTokensRatesAggregation extends SparkApplication[TokenRatesAggregationConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[TokenRatesAggregationConfig]): EntryPointConfig =
    EntryPointConfig("AggregateTokenRates")

  override protected def run(sparkSession: SparkSession, diConf: DIConfiguration,
                             configuration: TokenRatesAggregationConfig)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val container = new TokenRatesAggregationDriverComponent with BaseComponent {
      override implicit def spark: SparkSession = sparkSession
      override def diConfiguration: DIConfiguration = diConf
      override def tokenListScraper: EthereumTokensOps.TokenListScraper = EthereumTokensOps.coinMarketCapScraper
    }
    container.driver.run(configuration)
  }
}