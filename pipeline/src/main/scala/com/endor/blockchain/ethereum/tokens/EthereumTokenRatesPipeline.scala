package com.endor.blockchain.ethereum.tokens

import com.endor.storage.io.IOHandler
import com.endor.blockchain.ethereum.tokens.EthereumTokensOps._
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
        lower(regexp_replace($"name", " ", "-")) as "rateName",
        lower(regexp_replace($"symbol", " ", "-")) as "rateSymbol",
        $"price_usd" cast DataTypes.DoubleType as "price",
        $"market_cap_usd" cast DataTypes.DoubleType as "marketCap",
        $"last_updated" cast DataTypes.LongType cast DataTypes.TimestampType as "timestamp"
      )
    val nameToNameMatch = $"rateName" equalTo $"metaName"
    val nameToSymbolMatch = $"rateName" equalTo $"metaSymbol"
    val symbolToNameMatch = $"rateSymbol" equalTo $"metaName"
    val tokenNames = scrapeTokenList()
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
