package com.endor.blockchain.ethereum.tokens

import java.sql.Date

import com.endor.blockchain.ethereum.tokens.ratesaggregation._
import com.endor.blockchain.ethereum.tokens.EthereumTokensOps._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
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
    process(config).write.mode(SaveMode.Overwrite).parquet(config.outputPath)
  }

  def process(config: TokenRatesAggregationConfig): Dataset[AggregatedRates] = {
    import spark.implicits._
    val tokens = scrapeTokenList()

    val facts = spark.read.parquet(config.ratesFactsPath).as[RateRow]
    val factsFilterUdf = {
      val bc = spark.sparkContext.broadcast(tokens.toSet)
      udf(bc.value.contains _)
    }
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
      .na.drop()
      .select(AggregatedRates.encoder.schema.map(_.name).map(col): _*)
      .where(factsFilterUdf(trimNameUdf(normalizeNameUdf($"rateName"))))
      .as[AggregatedRates]

    val snapFilterUdf = {
      val bc = spark.sparkContext.broadcast(tokens.map(_.replace(" ", "-")).toSet)
      udf(bc.value.contains _)
    }
    val snapshotRates = spark.read.parquet(config.ratesSnapshotPath)
      .withColumn("date", to_date($"date"))
      .na.drop()
      .select(AggregatedRates.encoder.schema.map(_.name).map(col): _*)
      .where(snapFilterUdf(trimNameUdf(normalizeNameUdf($"rateName"))))
      .as[AggregatedRates]
    aggregatedFacts
      .union(snapshotRates)
  }
}
