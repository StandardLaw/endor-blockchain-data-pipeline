package com.endor.blockchain.ethereum.rates

import java.sql.Timestamp

import com.endor.DataKey
import com.endor.infra.SparkSessionComponent
import com.endor.storage.dataset.{DatasetStore, DatasetStoreComponent}
import com.endor.storage.sources._
import com.endor.blockchain.ethereum.ratesaggregation._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import play.api.libs.json.{Json, OFormat}

final case class AggregatedRates(date: Timestamp, open: Double, high: Double, low: Double, close: Double,
                                 marketCap: Option[Double])

object AggregatedRates {
  implicit val encoder: Encoder[AggregatedRates] = Encoders.product[AggregatedRates]
}

final case class EthereumRatesAggregationConfig(ratesFactsKey: DataKey[RateRow],
                                                ratesSnapshotKey: DataKey[AggregatedRates],
                                                outputKey: DataKey[AggregatedRates])

object EthereumRatesAggregationConfig {
  implicit val format: OFormat[EthereumRatesAggregationConfig] = Json.format[EthereumRatesAggregationConfig]
}

class EthereumRatesAggregationDriver()(implicit spark: SparkSession, datasetStore: DatasetStore){

  def run(config: EthereumRatesAggregationConfig): Unit = {
    datasetStore.storeParquet(config.outputKey.inbox, process(config))
  }

  private def process(config: EthereumRatesAggregationConfig): Dataset[AggregatedRates] = {
    val aggregatedFacts = aggregateFacts(config)
    val snapshotRates = datasetStore
      .loadParquet(config.ratesSnapshotKey.onBoarded)
    snapshotRates
      .union(aggregatedFacts)
  }

  private def aggregateFacts(config: EthereumRatesAggregationConfig): Dataset[AggregatedRates] = {
    import spark.implicits._
    val facts = datasetStore.loadParquet(config.ratesFactsKey.onBoarded)
    facts
      .groupBy(to_date($"timestamp") as "date")
      .agg(
        max($"price") as "high",
        min($"price") as "low",
        open($"timestamp", $"price") as "open",
        close($"timestamp", $"price") as "close",
        avg($"marketCap") as "marketCap"
      )
      .select(AggregatedRates.encoder.schema.map(_.name).map(col): _*)
      .as[AggregatedRates]
  }
}

trait EthereumRatesAggregationDriverComponent {
  this: SparkSessionComponent with DatasetStoreComponent =>
  val driver: EthereumRatesAggregationDriver = new EthereumRatesAggregationDriver()
}