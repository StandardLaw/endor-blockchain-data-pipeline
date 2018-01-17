package com.endor.blockchain.ethereum.tokens

import com.endor.blockchain.ethereum.ByteArrayUtil
import com.endor.spark.blockchain._
import com.endor.spark.blockchain.ethereum.block.EthereumBlockHeader
import com.endor.spark.blockchain.ethereum.token.TokenTransferEvent
import com.endor.spark.blockchain.ethereum.token.metadata.{TokenMetadata, TokenMetadataScraper}
import com.endor.storage.io.IOHandler
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, functions => F}
import play.api.libs.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

final case class EthereumTokensPipelineConfig(input: String, blocksInput: String, output: String, metadataPath: String)

object EthereumTokensPipelineConfig {
  implicit val format: OFormat[EthereumTokensPipelineConfig] = Json.format[EthereumTokensPipelineConfig]
}

final case class BlockInfo(number: Long, timestamp: java.sql.Timestamp)

class EthereumTokensPipeline(scraper: TokenMetadataScraper, ioHandler: IOHandler)
                            (implicit spark: SparkSession) {
  private def createProcessedDs(events: Dataset[TokenTransferEvent], tokenMetadata: Dataset[TokenMetadata],
                                blocksInfo: Dataset[BlockInfo]): Dataset[ProcessedTokenTransaction] = {
    val joinedWithMeta = events
      .joinWith(tokenMetadata, F.lower(tokenMetadata("address")) equalTo F.lower(F.hex(events("contractAddress"))), "left")
    joinedWithMeta
      .joinWith(blocksInfo, blocksInfo("number") equalTo joinedWithMeta("_1.blockNumber"), "left")
      .map {
        case ((event, metadata), blockInfo) =>
          ProcessedTokenTransaction(
            event.contractAddress.hex,
            event.blockNumber,
            event.fromAddress.hex,
            event.toAddress.hex,
            ByteArrayUtil.convertByteArrayToDouble(event.value, Option(metadata).flatMap(_.decimals).getOrElse(18), 3),
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
      .except(metadataDs.select("address").as[String].map(_.toLowerCase))
      .collect()

    val newMetadata =
      missingContracts
        .map(scraper.scrapeAddress)
        .map(_.map(Option.apply).recover {
          case _ => None
        })
        .flatMap(Await.result(_, 1 minute))

    val newMetadataDs = spark
      .createDataset(newMetadata)

    newMetadataDs
      .filter((metadata: TokenMetadata)=> {
          val symbolAndDecimalsAreOk = for {
            s <- metadata.symbol
            _ <- metadata.decimals
          } yield !s.isEmpty

          symbolAndDecimalsAreOk.getOrElse(false)
      })
      .write
      .mode(SaveMode.Append)
      .parquet(config.metadataPath)

    metadataDs.union(newMetadataDs)
  }

  def run(config: EthereumTokensPipelineConfig)
         (implicit ec: ExecutionContext): Unit = {
    ioHandler.deleteFilesByPredicate(ioHandler.extractPathFromFullURI(config.output), _ => true)

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
      .parquet(config.metadataPath)
      .as[TokenMetadata]

    val updatedMetadata = scrapeMissingMetadata(config, parsedEvents, metadataDs)

    val newBlockHeaders = spark
      .read
      .parquet(config.blocksInput)
      .as[EthereumBlockHeader]
      .select($"number", $"timestamp" cast DataTypes.TimestampType as "timestamp")
      .as[BlockInfo]

    val processedDs = createProcessedDs(parsedEvents, updatedMetadata, newBlockHeaders)
    processedDs
      .write
      .mode(SaveMode.Append)
      .parquet(config.output)
  }
}
