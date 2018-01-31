package com.endor.blockchain.ethereum.tokens

import java.sql.Date

import com.endor.blockchain.ethereum.tokens.ratesaggregation._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import play.api.libs.json.{Json, OFormat}

final case class AggregatedRates(date: Date, rateName: String, rateSymbol: String,
                                 metaName: Option[String], metaSymbol: Option[String], address: Option[String],
                                 open: Double, high: Double, low: Double, close: Double, marketCap: Double)
final case class TokenRatesAggregationConfig(ratesFactsPath: String, ratesSnapshotPath: String, outputPath: String)

object TokenRatesAggregationConfig {
  implicit val format: OFormat[TokenRatesAggregationConfig] = Json.format[TokenRatesAggregationConfig]
}

object AggregatedRates {
  implicit val encoder: Encoder[AggregatedRates] = Encoders.product[AggregatedRates]
}

class TokenRatesAggregationDriver()
                                 (implicit spark: SparkSession){
  def run(config: TokenRatesAggregationConfig): Unit = {
    process(config).write.parquet(config.outputPath)
  }

  def process(config: TokenRatesAggregationConfig): Dataset[AggregatedRates] = {
    import spark.implicits._

    val facts = spark.read.parquet(config.ratesFactsPath).as[RateRow]
    val aggregatedFacts = facts
      .groupBy($"rateName", $"rateSymbol", to_date($"timestamp") as "date")
      .agg(
        first($"metaName") as "metaName",
        first($"metaSymbol") as "metaSymbol",
        first($"address") as "address",
        max($"price") as "high",
        min($"price") as "low",
        open($"timestamp", $"price") as "open",
        close($"timestamp", $"price") as "close",
        avg($"marketCap") as "marketCap"
      )
      .select(AggregatedRates.encoder.schema.map(_.name).map(col): _*)
      .as[AggregatedRates]
    val baseRatesSnapshot = spark.read.parquet(config.ratesSnapshotPath)
      .withColumn("date", to_date($"date"))
    val snapshotWithMarketCap = if(!baseRatesSnapshot.columns.contains("marketCap")) {
      baseRatesSnapshot
        .withColumn("marketCap", lit(0.0))
    } else {
      baseRatesSnapshot
    }
    val snapshotRates = snapshotWithMarketCap
      .select(AggregatedRates.encoder.schema.map(_.name).map(col): _*)
      .as[AggregatedRates]
    aggregatedFacts.union(snapshotRates)

  }
}
