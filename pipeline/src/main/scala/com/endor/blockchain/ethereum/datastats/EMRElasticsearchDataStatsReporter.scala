package com.endor.blockchain.ethereum.datastats

import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.{BaseComponent, DIConfiguration}
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration}
import com.endor.jobnik.JobnikSession
import org.apache.spark.sql.SparkSession


object EMRElasticsearchDataStatsReporter extends SparkApplication[ElasticsearchDataStatsConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[ElasticsearchDataStatsConfig]): EntryPointConfig =
    EntryPointConfig("ElasticsearchDataStatsReporter")

  override protected def run(sparkSession: SparkSession, diConf: DIConfiguration,
                             configuration: ElasticsearchDataStatsConfig)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val container = new ElasticsearchDataStatsReporterComponent with BaseComponent {
      override implicit def spark: SparkSession = sparkSession
      override def diConfiguration: DIConfiguration = diConf
    }
    container.driver.run(configuration)
  }
}
