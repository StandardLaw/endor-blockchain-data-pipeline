package com.endor.blockchain.ethereum.datastats

import com.endor.DataKey
import com.endor.blockchain.ethereum.transaction.ProcessedTransaction
import com.endor.infra.SparkSessionComponent
import com.endor.storage.dataset.{BatchLoadOption, DatasetStore, DatasetStoreComponent}
import com.endor.storage.sources._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.elasticsearch.spark.sql._
import play.api.libs.json.{Json, OFormat}

final case class BlockStats(blockNumber: Long, date: String, numTx: Int, addresses: Seq[String])
object BlockStats {
  implicit val format: OFormat[BlockStats] = Json.format
  implicit val encoder: Encoder[BlockStats] = Encoders.product
  val esType: String = "BlockStatsV1"

  def merge(left: BlockStats, right: BlockStats): BlockStats = {
    require(left.blockNumber == right.blockNumber)
    require(left.date == right.date)
    BlockStats(left.blockNumber, left.date, right.numTx + left.numTx, (left.addresses ++ right.addresses).distinct)
  }
}

final case class DatasetDefinition[T: Encoder](dataKey: DataKey[T], batchLoadOption: BatchLoadOption)

object DatasetDefinition {
  implicit def format[T: Encoder]: OFormat[DatasetDefinition[T]] = Json.format[DatasetDefinition[T]]
}

final case class ElasticsearchDataStatsConfig(ethereumTxDefinition: DatasetDefinition[ProcessedTransaction],
                                              elasticsearchIndex: String, esHost: String, esPort: Int)

object ElasticsearchDataStatsConfig {
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
    val onBoarded = datasetStore.loadParquet(config.ethereumTxDefinition.dataKey.onBoarded,
      batchLoadOption = config.ethereumTxDefinition.batchLoadOption)
    onBoarded
      .map(
        tx =>
          BlockStats(tx.blockNumber, tx.timestamp.toInstant.toString, 1, Seq(tx.sendAddress))
      )
      .groupByKey(_.blockNumber)
      .reduceGroups(BlockStats.merge _)
      .map(_._2)
      // transactions are counted twice.
      .map(results => BlockStats(results.blockNumber, results.date, results.numTx/2, results.addresses))
  }


  private def storeToES(config: ElasticsearchDataStatsConfig, results: Dataset[BlockStats]): Unit = {
    results.saveToEs(s"${config.elasticsearchIndex}/${BlockStats.esType}",
      Map(
        "es.nodes" -> config.esHost,
        "es.port" -> config.esPort.toString,
        "es.nodes.wan.only" -> true.toString
      )
    )
  }
}

trait ElasticsearchDataStatsReporterComponent {
  this: SparkSessionComponent with DatasetStoreComponent =>
  lazy val driver: ElasticsearchDataStatsReporter = new ElasticsearchDataStatsReporter()
}