package com.endor.blockchain.ethereum.tokens

import com.endor.DataKey
import com.endor.blockchain.ethereum.tokens.EthereumTokensOps.{TokenListScraper, _}
import com.endor.infra.SparkSessionComponent
import com.endor.storage.dataset.{DatasetStore, DatasetStoreComponent}
import com.endor.storage.io.{IOHandler, IOHandlerComponent}
import com.endor.storage.sources._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import play.api.libs.json.{Json, OFormat}


final case class RateRow(rateName: String, rateSymbol: String, price: Double, marketCap: Option[Double],
                         metaName: Option[String], metaSymbol: Option[String], address: Option[String],
                         timestamp: java.sql.Timestamp)

object RateRow {
  implicit val encoder: Encoder[RateRow] = Encoders.product[RateRow]
}

final case class EthereumTokenRatesPipelineConfig(inputPath: String, metadataPath: String, output: DataKey[RateRow])

object EthereumTokenRatesPipelineConfig {
  implicit val format: OFormat[EthereumTokenRatesPipelineConfig] = Json.format[EthereumTokenRatesPipelineConfig]
}

private[tokens] final case class RawRateRow(name: String, symbol: String, price_usd: Option[String],
                                            market_cap_usd: Option[String], last_updated: String)
private[tokens] final case class MetadataRow(name: String, symbol: String, address: String)

class EthereumTokenRatesPipeline(tokenListScraper: TokenListScraper)
                                (implicit spark: SparkSession, ioHandler: IOHandler, datasetStore: DatasetStore){
  def run(config: EthereumTokenRatesPipelineConfig): Unit = {
    if(ioHandler.getDataSize(config.inputPath) > 0) {
      val result = process(config)
      datasetStore.storeParquet(config.output.inbox, result, saveMode = SaveMode.Append)
    }
  }

  private[tokens] def process(config: EthereumTokenRatesPipelineConfig): Dataset[RateRow] = {
    import spark.implicits._
    val metadata = spark.read.parquet(config.metadataPath)
      .as[MetadataRow]
      .select(
        $"name" as "metaName",
        $"symbol" as "metaSymbol",
        $"address"
      )
    val rawRates = spark.read
      .json(config.inputPath)
      .as[RawRateRow]
      .where($"price_usd" isNotNull)
      .select(
        normalizeNameUdf($"name") as "rateName",
        normalizeNameUdf($"symbol") as "rateSymbol",
        $"price_usd" cast DataTypes.DoubleType as "price",
        $"market_cap_usd" cast DataTypes.DoubleType as "marketCap",
        $"last_updated" cast DataTypes.LongType cast DataTypes.TimestampType as "timestamp"
      )
    val nameToNameMatch = $"rateName" equalTo $"metaName"
    val nameToSymbolMatch = $"rateName" equalTo $"metaSymbol"
    val symbolToNameMatch = $"rateSymbol" equalTo $"metaName"
    val tokenNames = tokenListScraper.scrape()
    val factsFilterUdf = {
      val bc = spark.sparkContext.broadcast(tokenNames.toSet)
      udf(bc.value.contains _)
    }
    rawRates
      .join(metadata, nameToNameMatch || nameToSymbolMatch || symbolToNameMatch, "left")
      .na.fill("n-a")
      .where(factsFilterUdf(trimNameUdf(normalizeNameUdf($"rateName"))))
      .as[RateRow]
  }
}

trait EthereumTokenRatesPipelineComponent {
  this: SparkSessionComponent with DatasetStoreComponent with IOHandlerComponent =>
  def tokenListScraper: TokenListScraper
  final lazy val driver: EthereumTokenRatesPipeline = new EthereumTokenRatesPipeline(tokenListScraper)
}