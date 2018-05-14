package com.endor.blockchain.ethereum.datastats

import java.sql.Timestamp

import com.endor.DataKey
import com.endor.blockchain.ethereum.transaction.ProcessedTransaction
import com.endor.infra.SparkSessionComponent
import com.endor.storage.dataset.{DatasetStore, DatasetStoreComponent}
import com.endor.storage.sources._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.elasticsearch.spark.sql._
import com.endor.serialization._
import play.api.libs.json.{Json, OFormat}

final case class BlockStats(blockNumber: Long, timestamp: Timestamp, numTx: Int, addresses: Set[String])
object BlockStats {
  implicit val format: OFormat[BlockStats] = Json.format
  implicit val encoder: Encoder[BlockStats] = Encoders.product
  val esType: String = "BlockStatsV1"

  def merge(left: BlockStats, right: BlockStats): BlockStats = {
    require(left.blockNumber == right.blockNumber)
    require(left.timestamp == right.timestamp)
    BlockStats(left.blockNumber, left.timestamp, right.numTx + left.numTx, left.addresses | right.addresses)
  }
}


  final case class ElasticsearchDataStatsConfig(txKey: DataKey[ProcessedTransaction], elasticsearchIndex: String, esHost: String, esPort: Int)

object DataStatsConfig {
  implicit val format: OFormat[ElasticsearchDataStatsConfig] = Json.format[ElasticsearchDataStatsConfig]
}

class ElasticsearchDataStatsReporter()
                                    (implicit spark: SparkSession, datasetStore: DatasetStore) {


  def run(config: ElasticsearchDataStatsConfig): Unit = {
    val results = process(config)
    storeToES(config, results)
  }

  private def process(config: ElasticsearchDataStatsConfig): Dataset[BlockStats] = {
    val sess = spark
    import sess.implicits._
    val onboarded = datasetStore.loadParquet(config.txKey.onBoarded)
    onboarded
      .map(
        tx =>
          BlockStats(tx.blockNumber, tx.timestamp,1,Set(tx.sendAddress))
      )
      .groupByKey(_.blockNumber)
      .reduceGroups(BlockStats.merge _)
      .map(_._2)
      // transactions are counted twice.
      .map(a => BlockStats(a.blockNumber, a.timestamp, a.numTx/2, a.addresses))
  }


  private def storeToES(config: ElasticsearchDataStatsConfig, results: Dataset[BlockStats]): Unit = {
    results.saveToEs(s"${config.elasticsearchIndex}/${BlockStats.esType}",
      Map("es.nodes" -> config.esHost, "es.port" -> config.esPort.toString))
  }
}

trait ElasticsearchDataStatsReporterComponent {
  this: SparkSessionComponent with DatasetStoreComponent =>
  lazy val driver: ElasticsearchDataStatsReporter = new ElasticsearchDataStatsReporter()
}