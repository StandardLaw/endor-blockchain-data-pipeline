package com.endor.blockchain.ethereum.tokens

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.actor.ActorSystem
import com.endor.spark.blockchain.ethereum.token.TokenRatesFetcher
import com.endor.spark.blockchain.ethereum.token.metadata.TokenMetadata
import com.endor.storage.io.IOHandler
import org.apache.spark.sql.{SaveMode, SparkSession}
import play.api.libs.json.{OFormat, Json}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.io.Source

final case class EthereumTokenRatesPipelineConfig(metadataPath: String, lastFetchedPath: String, output: String)

object EthereumTokenRatesPipelineConfig {
  implicit val format: OFormat[EthereumTokenRatesPipelineConfig] = Json.format[EthereumTokenRatesPipelineConfig]
}

class EthereumTokenRatesPipeline(ioHandler: IOHandler)
                                (implicit spark: SparkSession){
  def run(config: EthereumTokenRatesPipelineConfig): Unit = {
    val lastFetchedDate = ioHandler
      .listFiles(ioHandler.extractPathFromFullURI(config.lastFetchedPath))
      .headOption
      .map(_.key)
      .map(ioHandler.loadRawData)
      .map(Source.fromInputStream)
      .map(_.mkString.trim)
      .map(Instant.parse)
      .getOrElse(Instant.parse("1970-01-01T00:00:00Z"))
      .truncatedTo(ChronoUnit.DAYS)
    val today = Instant.now().truncatedTo(ChronoUnit.DAYS)
    if (lastFetchedDate isBefore today) {
      import spark.implicits._

      spark.read
        .schema(TokenMetadata.encoder.schema)
        .parquet(config.metadataPath)
        .as[TokenMetadata]
        .select("symbol")
        .where($"symbol" isNotNull)
        .distinct()
        .as[String]
        .mapPartitions {
          it =>
            implicit val system: ActorSystem = ActorSystem()
            implicit val ec: ExecutionContext = ExecutionContext.global
            val fetcher = new TokenRatesFetcher()(system)
            it
              .flatMap(symbol => Await.result(fetcher.fetchRate(symbol), 1 minute))
        }
        .write
        .mode(SaveMode.Append)
        .parquet(config.output)
      ioHandler.saveRawData(today.toString, config.lastFetchedPath)
    }
  }
}
