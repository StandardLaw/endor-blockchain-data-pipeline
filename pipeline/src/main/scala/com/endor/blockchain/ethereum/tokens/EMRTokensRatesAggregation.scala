package com.endor.blockchain.ethereum.tokens

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration}
import com.endor.jobnik.JobnikSession
import org.apache.spark.sql.SparkSession

object EMRTokensRatesAggregation extends SparkApplication[TokenRatesAggregationConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[TokenRatesAggregationConfig]): EntryPointConfig =
    EntryPointConfig("AggregateTokenRates")

  override protected def run(sparkSession: SparkSession, configuration: TokenRatesAggregationConfig)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val driver = new TokenRatesAggregationDriver()(sparkSession)
    driver.run(configuration)
  }
}