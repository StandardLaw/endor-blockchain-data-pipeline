package com.endor.blockchain.ethereum.rates

import com.endor.DataKey
import com.endor.blockchain.ethereum.tokens.EthereumTokensOps._
import com.endor.infra.SparkSessionComponent
import com.endor.storage.dataset.{DatasetStore, DatasetStoreComponent}
import com.endor.storage.io.{IOHandler, IOHandlerComponent}
import com.endor.storage.sources._
import org.apache.spark.sql._
import org.apache.spark.sql.types.DataTypes
import play.api.libs.json.{Json, OFormat}


final case class RateRow(timestamp: java.sql.Timestamp, price: Double)

object RateRow {
  implicit val encoder: Encoder[RateRow] = Encoders.product[RateRow]
}

final case class EthereumRatesPipelineConfig(inputPath: String, output: DataKey[RateRow])

object EthereumRatesPipelineConfig {
  implicit val format: OFormat[EthereumRatesPipelineConfig] = Json.format[EthereumRatesPipelineConfig]
}

private[rates] final case class RawRateRow(name: String, symbol: String, price_usd: Option[String],
                                            market_cap_usd: String, last_updated: String)

class EthereumRatesPipeline()(implicit spark: SparkSession, ioHandler: IOHandler, datasetStore: DatasetStore){
  def run(config: EthereumRatesPipelineConfig): Unit = {
    if(ioHandler.getDataSize(config.inputPath) > 0) {
      val result = process(config)
      datasetStore.storeParquet(config.output.inbox, result, saveMode = SaveMode.Append)
    }
  }

  private[rates] def process(config: EthereumRatesPipelineConfig): Dataset[RateRow] = {
    import spark.implicits._
    spark.read
      .json(config.inputPath)
      .as[RawRateRow]
      .where($"price_usd" isNotNull)
      .where(normalizeNameUdf($"name") equalTo "ethereum")
      .select(
        $"last_updated" cast DataTypes.LongType cast DataTypes.TimestampType as "timestamp",
        $"price_usd" cast DataTypes.DoubleType as "price"
      )
      .as[RateRow]

  }
}

trait EthereumRatesPipelineComponent {
  this: SparkSessionComponent with DatasetStoreComponent with IOHandlerComponent =>
  final lazy val driver: EthereumRatesPipeline = new EthereumRatesPipeline()
}