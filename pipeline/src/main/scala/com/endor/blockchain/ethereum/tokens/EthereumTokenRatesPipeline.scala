package com.endor.blockchain.ethereum.tokens

import com.endor.storage.io.IOHandler
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import play.api.libs.json.{Json, OFormat}


final case class RateRow(rateName: String, rateSymbol: String, price: Double, metaName: Option[String],
                         metaSymbol: Option[String], address: Option[String], timestamp: java.sql.Timestamp)

object RateRow {
  implicit val encoder: Encoder[RateRow] = Encoders.product[RateRow]
}

final case class EthereumTokenRatesPipelineConfig(inputPath: String, metadataPath: String, output: String)

object EthereumTokenRatesPipelineConfig {
  implicit val format: OFormat[EthereumTokenRatesPipelineConfig] = Json.format[EthereumTokenRatesPipelineConfig]
}

class EthereumTokenRatesPipeline(ioHandler: IOHandler)
                                (implicit spark: SparkSession){
  def run(config: EthereumTokenRatesPipelineConfig): Unit = {
    if(ioHandler.getDataSize(config.inputPath) > 0) {
      val result = process(config)
      result.write.mode(SaveMode.Append).parquet(config.output)
    }
  }

  private[tokens] def process(config: EthereumTokenRatesPipelineConfig): Dataset[RateRow] = {
    import spark.implicits._
    val metadata = spark.read.parquet(config.metadataPath)
      .select(
        $"name" as "metaName",
        $"symbol" as "metaSymbol",
        $"address"
      )
    val rawRates = spark.read
      .json(config.inputPath)
      .where($"price_usd" isNotNull)
      .select(
        $"name" as "rateName",
        $"symbol" as "rateSymbol",
        $"price_usd" cast DataTypes.DoubleType as "price",
        $"market_cap_usd" cast DataTypes.DoubleType as "marketCap",
        $"last_updated" cast DataTypes.LongType cast DataTypes.TimestampType as "timestamp"
      )
    val nameToNameMatch = lower($"rateName") equalTo lower($"metaName")
    val nameToSymbolMatch = lower($"rateName") equalTo lower($"metaSymbol")
    val symbolToNameMatch = lower($"rateSymbol") equalTo lower($"metaName")
    val tokenList = spark.sparkContext.broadcast(CoinMarketCapTokeList.get().toSet)
    rawRates
      .join(metadata, nameToNameMatch || nameToSymbolMatch || symbolToNameMatch, "left")
      .na.fill("n-a")
      .as[RateRow]
      .filter((row: RateRow) => tokenList.value.contains(row.rateName))

  }
}
