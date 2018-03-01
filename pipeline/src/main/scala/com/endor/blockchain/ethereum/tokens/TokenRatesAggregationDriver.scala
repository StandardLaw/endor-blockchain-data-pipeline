package com.endor.blockchain.ethereum.tokens

import java.sql.{Date, Timestamp}

import com.endor.DataKey
import com.endor.blockchain.ethereum.tokens.EthereumTokensOps._
import com.endor.blockchain.ethereum.ratesaggregation._
import com.endor.infra.SparkSessionComponent
import com.endor.storage.dataset.{DatasetStore, DatasetStoreComponent}
import com.endor.storage.sources._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import play.api.libs.json.{Json, OFormat}

private[tokens] final case class RatesSnapshotRow(rateName: String, rateSymbol: String, date: Option[Timestamp],
                                          open: Option[Double], high: Option[Double], low: Option[Double],
                                          close: Option[Double], marketCap: Option[Double])

object RatesSnapshotRow {
  implicit val encoder: Encoder[RatesSnapshotRow] = Encoders.product[RatesSnapshotRow]
}

final case class AggregatedRates(date: Date, rateName: String, rateSymbol: String,
                                 metaName: Option[String], metaSymbol: Option[String], address: Option[String],
                                 open: Double, high: Double, low: Double, close: Double, marketCap: Option[Double])

object AggregatedRates {
  implicit val encoder: Encoder[AggregatedRates] = Encoders.product[AggregatedRates]
}

final case class TokenRatesAggregationConfig(ratesFactsKey: DataKey[RateRow],
                                             ratesSnapshotKey: DataKey[RatesSnapshotRow],
                                             metadataPath: String, outputKey: DataKey[AggregatedRates])

object TokenRatesAggregationConfig {
  implicit val format: OFormat[TokenRatesAggregationConfig] = Json.format[TokenRatesAggregationConfig]
}

class TokenRatesAggregationDriver(tokenListScraper: TokenListScraper)
                                 (implicit spark: SparkSession, datasetStore: DatasetStore){

  def run(config: TokenRatesAggregationConfig): Unit = {
    datasetStore.storeParquet(config.outputKey.inbox, process(config))
  }

  private def process(config: TokenRatesAggregationConfig): Dataset[AggregatedRates] = {
    val tokens = tokenListScraper.scrape()
    val aggregatedFacts = aggregateFacts(config, tokens)
    val snapshotRates = createAggregatedDsFromSnapshotAndMetadata(config, tokens)
    aggregatedFacts
      .union(snapshotRates)
  }

  private def aggregateFacts(config: TokenRatesAggregationConfig, tokens: Seq[String]) = {
    import spark.implicits._
    val facts = datasetStore.loadParquet(config.ratesFactsKey.onBoarded)
    val factsFilterUdf = {
      val bc = spark.sparkContext.broadcast(tokens.toSet)
      udf(bc.value.contains _)
    }
    val aggregatedFacts = facts
      .groupBy($"rateName", $"rateSymbol", $"address", to_date($"timestamp") as "date")
      .agg(
        first($"metaName") as "metaName",
        first($"metaSymbol") as "metaSymbol",
        max($"price") as "high",
        min($"price") as "low",
        open($"timestamp", $"price") as "open",
        close($"timestamp", $"price") as "close",
        avg($"marketCap") as "marketCap"
      )
      .select(AggregatedRates.encoder.schema.map(_.name).map(col): _*)
      .where(factsFilterUdf(trimNameUdf(normalizeNameUdf($"rateName"))))
      .as[AggregatedRates]
    aggregatedFacts
  }

  private def createAggregatedDsFromSnapshotAndMetadata(config: TokenRatesAggregationConfig, tokens: Seq[String]) = {
    import spark.implicits._
    val snapFilterUdf = {
      val bc = spark.sparkContext.broadcast(tokens.map(_.replace(" ", "-")).toSet)
      udf(bc.value.contains _)
    }
    val metadata = spark.read.parquet(config.metadataPath).select($"name" as "metaName",
      $"symbol" as "metaSymbol", $"address")
    val nameToNameMatch = $"rateName" equalTo $"metaName"
    val nameToSymbolMatch = $"rateName" equalTo $"metaSymbol"
    val symbolToNameMatch = $"rateSymbol" equalTo $"metaName"
    val snapshotRates = datasetStore.loadParquet(config.ratesSnapshotKey.onBoarded)
      .na.drop(Seq("date", "open", "high", "low", "close"))
      .withColumn("date", to_date($"date"))
      .join(metadata, nameToNameMatch || nameToSymbolMatch || symbolToNameMatch, "left")
      .na.fill(Map("metaName" -> "n-a", "metaSymbol" -> "n-a", "address" -> "n-a"))
      .where(snapFilterUdf(trimNameUdf(normalizeNameUdf($"rateName"))))
      .as[AggregatedRates]
    snapshotRates
  }
}

trait TokenRatesAggregationDriverComponent {
  this: SparkSessionComponent with DatasetStoreComponent =>
  def tokenListScraper: TokenListScraper
  val driver: TokenRatesAggregationDriver = new TokenRatesAggregationDriver(tokenListScraper)
}