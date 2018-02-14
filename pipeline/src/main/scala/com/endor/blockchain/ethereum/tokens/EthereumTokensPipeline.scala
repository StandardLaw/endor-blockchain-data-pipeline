package com.endor.blockchain.ethereum.tokens

import akka.actor.ActorSystem
import ch.qos.logback.classic.{Logger, LoggerContext}
import com.endor.DataKey
import com.endor.blockchain.ethereum.ByteArrayUtil
import com.endor.infra.{LoggingComponent, SparkSessionComponent}
import com.endor.spark.blockchain._
import com.endor.spark.blockchain.ethereum.block.EthereumBlockHeader
import com.endor.spark.blockchain.ethereum.token.TokenTransferEvent
import com.endor.spark.blockchain.ethereum.token.metadata._
import com.endor.storage.dataset.{DatasetStore, DatasetStoreComponent}
import com.endor.storage.io.{IOHandler, IOHandlerComponent}
import com.endor.storage.sources._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, functions => F}
import org.web3j.protocol.Web3j
import org.web3j.protocol.http.HttpService
import play.api.libs.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

final case class EthereumTokensPipelineConfig(input: String, blocksInput: String,
                                              output: DataKey[ProcessedTokenTransaction],
                                              metadataCachePath: String,
                                              metadataOutputPath: DataKey[TokenMetadata])

object EthereumTokensPipelineConfig {
  implicit val format: OFormat[EthereumTokensPipelineConfig] = Json.format[EthereumTokensPipelineConfig]
}

final case class BlockInfo(number: Long, timestamp: java.sql.Timestamp)

class EthereumTokensPipeline(scraper: TokenMetadataScraper)
                            (implicit spark: SparkSession, loggerFactory: LoggerContext,
                             ioHandler: IOHandler, datasetStore: DatasetStore) {
  private lazy val logger: Logger = loggerFactory.getLogger(this.getClass)

  private def createProcessedDs(events: Dataset[TokenTransferEvent], tokenMetadata: Dataset[TokenMetadata],
                                blocksInfo: Dataset[BlockInfo]): Dataset[ProcessedTokenTransaction] = {
    val joinedWithMeta = events
      .joinWith(F.broadcast(tokenMetadata), F.lower(tokenMetadata("address")) equalTo F.lower(F.hex(events("contractAddress"))), "left")
    joinedWithMeta
      .joinWith(blocksInfo, blocksInfo("number") equalTo joinedWithMeta("_1.blockNumber"), "left")
      .map {
        case ((event, metadata), blockInfo) =>
          val tokenDecimals = Option(metadata).flatMap(_.decimals).map(Math.max(_, 3)).getOrElse(18)
          val decimalPrecision = Math.min(tokenDecimals, EthereumTokensPipeline.decimalPrecisionForValues)
          ProcessedTokenTransaction(
            event.contractAddress.hex,
            event.blockNumber,
            event.fromAddress.hex,
            event.toAddress.hex,
            ByteArrayUtil.convertByteArrayToDouble(event.value, tokenDecimals - 3, decimalPrecision),
            event.transactionHash.hex,
            event.transactionIndex,
            blockInfo.timestamp)
      }
  }

  private def scrapeMissingMetadata(config: EthereumTokensPipelineConfig, parsedEvents: Dataset[TokenTransferEvent],
                                    metadataDs: Dataset[TokenMetadata])
                                   (implicit ec: ExecutionContext): Dataset[TokenMetadata] = {
    import parsedEvents.sparkSession.implicits._

    val missingContracts = parsedEvents
      .select("contractAddress")
      .as[Array[Byte]]
      .map(_.hex.toLowerCase)
      .distinct
      .except(F.broadcast(metadataDs).select("address").as[String].map(_.toLowerCase))
      .collect()
      .toSeq

    logger.info(s"Found ${missingContracts.length} missing contract metadatas")

    val newMetadata =
      missingContracts
        .zipWithIndex
        .flatMap {
          case (address, idx)=>
            logger.info(s"Scraping $address - $idx / ${missingContracts.length}")
            val eventualResult = scraper.scrapeAddress(address)
              .map(Option.apply)
              .recover {
                case _ => None
              }
            val result = Await.result(eventualResult, 1 minute)
            logger.debug("Done")
            result
        }
      .map(meta =>
        meta.copy(
          name = meta.name.map(EthereumTokensOps.normalizeName),
          symbol = meta.symbol.map(EthereumTokensOps.normalizeName)
        )
      )

    logger.info(s"Found ${newMetadata.length} new contract metadatas")

    val newMetadataDs = spark
      .createDataset(newMetadata)

    newMetadataDs
      .filter((metadata: TokenMetadata) => {
        val symbolAndDecimalsAreOk = for {
          n <- metadata.name
          s <- metadata.symbol
          _ <- metadata.decimals
        } yield !n.isEmpty && !s.isEmpty

        symbolAndDecimalsAreOk.getOrElse(false)
      })
      .write
      .mode(SaveMode.Append)
      .parquet(config.metadataCachePath)

    metadataDs.union(newMetadataDs)
  }

  def run(config: EthereumTokensPipelineConfig)
         (implicit ec: ExecutionContext): Unit = {
    ioHandler.deleteFilesByPredicate(config.output.inbox.path, _ => true)

    import spark.implicits._

    val rawEvents = spark
      .read
      .json(config.input)
    val parsedEvents = rawEvents
      .map {
        row =>
          TokenTransferEvent(
            row.getAs[String]("contractAddress").bytes,
            row.getAs[String]("fromAddress").bytes,
            row.getAs[String]("toAddress").bytes,
            row.getAs[String]("value").bytes,
            row.getAs[Long]("blockNumber"),
            row.getAs[String]("transactionHash").bytes,
            row.getAs[Long]("transactionIndex").toInt
          )
      }
    val metadataDs = spark
      .read
      .schema(TokenMetadata.encoder.schema)
      .parquet(config.metadataCachePath)
      .as[TokenMetadata]

    val updatedMetadata = scrapeMissingMetadata(config, parsedEvents, metadataDs)

    datasetStore.storeParquet(
      config.metadataOutputPath.inbox,
      updatedMetadata.filter((metadata: TokenMetadata) => metadata.symbol.exists(!_.isEmpty))
    )

    val newBlockHeaders = spark
      .read
      .parquet(config.blocksInput)
      .as[EthereumBlockHeader]
      .select($"number", $"timestamp" cast DataTypes.TimestampType as "timestamp")
      .as[BlockInfo]

    val processedDs = createProcessedDs(parsedEvents, updatedMetadata, newBlockHeaders)
    datasetStore.storeParquet(config.output.inbox, processedDs, saveMode = SaveMode.Append)
  }
}

object EthereumTokensPipeline {
  val decimalPrecisionForValues: Int = 3
}

trait EthereumTokenPipelineComponent {
  this: LoggingComponent with SparkSessionComponent with DatasetStoreComponent with IOHandlerComponent =>

  private val scraper = {
    implicit val actorSystem: ActorSystem = ActorSystem()
    val web3j = Web3j.build(new HttpService(s"http://geth.endorians.com:8545/"))
    new CachedTokenMetadataScraper(
      new CompositeTokenMetadataScraper(
        new Web3TokenMetadataScraper(web3j),
        new EthplorerTokenMetadataScraper("freekey"),
        new EtherscanTokenMetadataScraper()
      )
    )
  }

  lazy val driver: EthereumTokensPipeline = new EthereumTokensPipeline(scraper)
}