package com.endor.blockchain.ethereum.block

import com.endor.spark.blockchain.ethereum._
import com.endor.storage.io.IOHandler
import org.apache.spark.sql.{SaveMode, SparkSession}
import play.api.libs.json.{Json, OFormat}

final case class EthereumBlocksPipelineConfig(input: String, output: String)

object EthereumBlocksPipelineConfig {
  implicit val format: OFormat[EthereumBlocksPipelineConfig] = Json.format[EthereumBlocksPipelineConfig]
}

class EthereumBlocksPipeline(ioHandler: IOHandler)(implicit spark: SparkSession) {
  def run(config: EthereumBlocksPipelineConfig): Unit = {
    ioHandler.deleteFilesByPredicate(ioHandler.extractPathFromFullURI(config.output), _ => true)
    spark
      .read
      .ethereum(config.input)
      .map(_.ethereumBlockHeader)
      .write
      .mode(SaveMode.Append)
      .parquet(config.output)
  }
}
