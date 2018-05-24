package com.endor.blockchain.ethereum.datastats

import java.sql.{Date, Timestamp}
import java.time.Instant
import java.time.temporal.ChronoUnit

import com.endor.DataKey
import com.endor.blockchain.ethereum.tokens.AggregatedRates
import com.endor.blockchain.ethereum.transaction.ProcessedTransaction
import com.endor.infra.SparkSessionComponent
import com.endor.storage.dataset.{BatchLoadOption, DatasetStore, DatasetStoreComponent}
import com.endor.storage.sources._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, functions => F}
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.spark.sql._
import play.api.libs.json.{Json, OFormat}

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

final case class EthereumBlockStatsV1(blockNumber: Long, date: String, numTx: Int, addresses: Seq[String])
object EthereumBlockStatsV1 {
  implicit val format: OFormat[EthereumBlockStatsV1] = Json.format
  implicit val encoder: Encoder[EthereumBlockStatsV1] = Encoders.product

  def merge(left: EthereumBlockStatsV1, right: EthereumBlockStatsV1): EthereumBlockStatsV1 = {
    require(left.blockNumber == right.blockNumber)
    require(left.date == right.date)
    EthereumBlockStatsV1(left.blockNumber, left.date, right.numTx + left.numTx, (left.addresses ++ right.addresses).distinct)
  }
}

final case class DatasetDefinition[T: Encoder](dataKey: DataKey[T], batchLoadOption: BatchLoadOption)

object DatasetDefinition {
  implicit def format[T: Encoder]: OFormat[DatasetDefinition[T]] = Json.format[DatasetDefinition[T]]
}

final case class ElasticsearchDataStatsConfig(ethereumTxDefinition: DatasetDefinition[ProcessedTransaction],
                                              etherRatesDefinition: DatasetDefinition[AggregatedRates],
                                              elasticsearchIndex: String, esHost: String, esPort: Int)

object ElasticsearchDataStatsConfig {
  implicit val format: OFormat[ElasticsearchDataStatsConfig] = Json.format[ElasticsearchDataStatsConfig]
}

trait EsType[T] {
  protected def _indexName: String
  final def indexName: String = _indexName.toLowerCase()
  def typeVersion: String

  def getEsIndex: String = s"$indexName/$typeVersion"
}

object EsType {
  private val classNameRegex = """([\w]+)(V\d+)""".r
  implicit def esType[T](implicit classTag: ClassTag[T]): EsType[T] = {
    classTag.runtimeClass.getSimpleName match {
      case classNameRegex(index, version) =>
        new  EsType[T] {
          override val _indexName: String = index
          override val typeVersion: String = version
        }
      case className =>
        new  EsType[T] {
          override val _indexName: String = className
          override val typeVersion: String = "na"
        }
    }
  }
}

class ElasticsearchDataStatsReporter()
                                    (implicit spark: SparkSession, datasetStore: DatasetStore) {
  def run(config: ElasticsearchDataStatsConfig): Unit = {
    val etherData = processEthereum(config)
    storeToES(config, etherData)
    val ratesData = processRates(config)
    storeToES(config, ratesData)
  }

  private def processEthereum(config: ElasticsearchDataStatsConfig): Dataset[EthereumBlockStatsV1] = {
    val sess = spark
    import sess.implicits._
    val onBoarded = datasetStore.loadParquet(config.ethereumTxDefinition.dataKey.onBoarded,
      batchLoadOption = config.ethereumTxDefinition.batchLoadOption)
    onBoarded
      .map(
        tx =>
          EthereumBlockStatsV1(tx.blockNumber, tx.timestamp.toInstant.toString, 1, Seq(tx.sendAddress))
      )
      .groupByKey(_.blockNumber)
      .reduceGroups(EthereumBlockStatsV1.merge _)
      .map(_._2)
      // transactions are counted twice.
      .map(results => EthereumBlockStatsV1(results.blockNumber, results.date, results.numTx/2, results.addresses))
  }

  private def processRates(config: ElasticsearchDataStatsConfig)
                          (implicit esType: EsType[AggregatedRates]): Dataset[AggregatedRates] = {
    val sess = spark
    import sess.implicits._
    val millisInDay = 86400000 // 3600 * 24 *  1000
    val yesterday = new Date(Instant.now().truncatedTo(ChronoUnit.DAYS).toEpochMilli - millisInDay)
    val maxDateInES = Try {
      spark.esDF(esType.getEsIndex, createEsConfig(config))
        .withColumn("date", F.to_date($"date" / 1000 cast DataTypes.TimestampType))
        .agg(F.max("date"))
        .as[Option[Date]]
        .head()
    } match  {
      case Success(value) => value
      case Failure(_: EsHadoopIllegalArgumentException) => None
      case Failure(err) => throw err
    }
    val loaded = datasetStore.loadParquet(config.etherRatesDefinition.dataKey.onBoarded,
      batchLoadOption = config.etherRatesDefinition.batchLoadOption)
    maxDateInES.map(maxDate => loaded.filter($"date" > maxDate)).getOrElse(loaded)
      .filter($"date" <= yesterday)
  }


  private def createEsConfig(config: ElasticsearchDataStatsConfig): Map[String, String] =
    Map(
      "es.nodes" -> config.esHost,
      "es.port" -> config.esPort.toString,
      "es.nodes.wan.only" -> true.toString
    )

  private def storeToES[T: Encoder: ClassTag](config: ElasticsearchDataStatsConfig, results: Dataset[T])
                                             (implicit esType: EsType[T]): Unit = {
    val now_ts = Timestamp.from(Instant.now()).toString
    val withTS = results.withColumn("published_on",(F.lit(now_ts)))
    withTS.coalesce(10).saveToEs(esType.getEsIndex, createEsConfig(config))
  }
}

trait ElasticsearchDataStatsReporterComponent {
  this: SparkSessionComponent with DatasetStoreComponent =>
  lazy val driver: ElasticsearchDataStatsReporter = new ElasticsearchDataStatsReporter()
}
