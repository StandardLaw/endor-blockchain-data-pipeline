package com.endor.blockchain.ethereum.rates

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration}
import com.endor.infra.{BaseComponent, DIConfiguration}
import com.endor.jobnik.JobnikSession
import org.apache.spark.sql.SparkSession

object EMREthereumRatesAggregation extends SparkApplication[EthereumRatesAggregationConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[EthereumRatesAggregationConfig]): EntryPointConfig =
    EntryPointConfig("AggregateTokenRates")

  override protected def run(sparkSession: SparkSession, diConf: DIConfiguration,
                             configuration: EthereumRatesAggregationConfig)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val container = new EthereumRatesAggregationDriverComponent with BaseComponent {
      override implicit def spark: SparkSession = sparkSession
      override def diConfiguration: DIConfiguration = diConf
    }
    container.driver.run(configuration)
  }
}