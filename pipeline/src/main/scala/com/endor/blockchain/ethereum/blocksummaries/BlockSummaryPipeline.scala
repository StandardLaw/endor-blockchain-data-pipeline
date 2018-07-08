package com.endor.blockchain.ethereum.blocksummaries

import java.util.Properties

import akka.actor.ActorSystem
import ch.qos.logback.classic.{Logger, LoggerContext}
import com.endor.DataKey
import com.endor.blockchain.ethereum.ByteArrayUtil
import com.endor.blockchain.ethereum.tokens.EthereumTokensOps
import com.endor.context.Context
import com.endor.infra.{LoggingComponent, SparkSessionComponent}
import com.endor.spark.blockchain.ethereum.token.metadata._
import com.endor.storage.dataset.{DatasetStore, DatasetStoreComponent}
import com.endor.storage.io.{IOHandler, IOHandlerComponent}
import com.endor.storage.sources._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, functions => F}
import org.ethereum.core.{BlockSummary => JBlockSummary}
import org.web3j.protocol.Web3j
import org.web3j.protocol.http.HttpService
import play.api.libs.json.{Json, OFormat}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

final case class DatabaseConfig(name: String, host: String, user: String, password: String, port: Int) {
  def connectionString: String = s"jdbc:mysql://$user:$password@$host:$port/$name"
  def connectionProperties: Properties = {
    val props = new Properties()
    props.put("user", user)
    props.put("password", password)
    props.put("driver", "com.mysql.cj.jdbc.Driver")
    props
  }
}

object DatabaseConfig {
  implicit val format: OFormat[DatabaseConfig] = Json.format
}

final case class BlockSummaryPipelineConfiguration(databaseConfig: DatabaseConfig,
                                                   transactionsOutput: DataKey[Transaction],
                                                   tokenTransactionsOutputs: Seq[DataKey[TokenTransaction]],
                                                   rewardsOutput: DataKey[BlockReward],
                                                   metadataCachePath: String,
                                                   metadataOutputPath: Option[DataKey[TokenMetadata]])

object BlockSummaryPipelineConfiguration {
  implicit val format: OFormat[BlockSummaryPipelineConfiguration] = Json.format
}

class BlockSummaryPipeline(scraper: TokenMetadataScraper)
                          (implicit spark: SparkSession, datasetStore: DatasetStore,
                           loggerFactory: LoggerContext, ioHandler: IOHandler) {
  import spark.implicits._

  private lazy val logger: Logger = loggerFactory.getLogger(this.getClass)

  def loadDataFromSQL(configuration: BlockSummaryPipelineConfiguration)
                     (implicit context: Context): Dataset[Array[Byte]] = {
    val highestLoadedBlock = context.callsiteContext.enrichContext("Find highest loaded block") {
      Try(datasetStore.loadParquet(configuration.transactionsOutput.onBoarded))
        .toOption
        .flatMap(_.agg(F.max("blockNumber")).as[Option[Long]].head)
        .getOrElse(-1L)
    }
    val databaseConfig = configuration.databaseConfig
    val highestAvailableBlock = context.callsiteContext.enrichContext("Find highest available block") {
      spark.read.jdbc(databaseConfig.connectionString,
        "(select max(blockNumber) from summaries) max_block", databaseConfig.connectionProperties)
        .as[Long].collect().headOption.getOrElse(0L)
    }
    context.callsiteContext.enrichContext("Load data from MySQL") {
      spark.read.jdbc(
        databaseConfig.connectionString,
        s"(SELECT blockNumber, data FROM summaries WHERE blockNumber > $highestLoadedBlock AND blockNumber <= $highestAvailableBlock) s",
        "blockNumber",
        highestLoadedBlock,
        highestAvailableBlock,
        if (context.testMode) 2 else 200,
        databaseConfig.connectionProperties)
        .select("data")
        .as[Array[Byte]]
    }
  }

  def run(configuration: BlockSummaryPipelineConfiguration)
         (implicit ec: ExecutionContext, context: Context): Unit = {
    val data = loadDataFromSQL(configuration)
    val parsedSummaries = data
      .map(x => {
        BlockSummary.fromJBlockSummary(new JBlockSummary(x))
      })
      .cache()
    val transactions = parsedSummaries.flatMap(_.ethereumTransactions)
    val tokenTransactions = parsedSummaries.flatMap(_.erc20Transactions)
    val rewards = parsedSummaries.flatMap(_.rewards)
    val tokenMetadata = context.callsiteContext.enrichContext("Collect token metadata") {
      getTokenMetadata(tokenTransactions, configuration).collect()
        .map(x => x.address -> x)
        .toMap
    }
    val broadcastedMetadata = spark.sparkContext.broadcast(tokenMetadata)
    val ttxWithTranslatedValues = tokenTransactions.map(ttx => {
      val tokenDecimals = broadcastedMetadata.value.get(ttx.contractAddress)
        .flatMap(_.decimals).map(Math.max(_, 3)).getOrElse(18)
      val decimalPrecision = Math.min(tokenDecimals, BlockSummaryPipeline.decimalPrecisionForValues)
      TokenTransaction(
        ttx.contractAddress,
        ttx.blockNumber,
        ttx.fromAddress,
        ttx.toAddress,
        ByteArrayUtil.convertByteArrayToDouble(ttx.value, tokenDecimals - 3, decimalPrecision),
        ttx.transactionHash,
        ttx.transactionIndex,
        ttx.timestamp,
        flipped = false
      )
    })
      .flatMap(ttx => Seq(ttx, ttx.flip))
    configuration.tokenTransactionsOutputs.foreach(tokenTransactionsOutput => {
      ioHandler.deleteFilesByPredicate(tokenTransactionsOutput.inbox.path, _ => true)
      context.callsiteContext.enrichContext(s"Store token transactions for ${tokenTransactionsOutput.customer.id}") {
        datasetStore.storeParquet(tokenTransactionsOutput.inbox, ttxWithTranslatedValues)
      }
    })
    ioHandler.deleteFilesByPredicate(configuration.transactionsOutput.inbox.path, _ => true)
    context.callsiteContext.enrichContext("Store ethereum transactions") {
      datasetStore.storeParquet(configuration.transactionsOutput.inbox, transactions)
    }
    ioHandler.deleteFilesByPredicate(configuration.rewardsOutput.inbox.path, _ => true)
    context.callsiteContext.enrichContext("Store ethereum rewards") {
      datasetStore.storeParquet(configuration.rewardsOutput.inbox, rewards)
    }
  }

  private def scrapeMissingMetadata(config: BlockSummaryPipelineConfiguration,
                                    parsedEvents: Dataset[TokenTransactionRawValue],
                                    metadataDs: Dataset[TokenMetadata])
                                   (implicit ec: ExecutionContext, context: Context): Dataset[TokenMetadata] = {
    import parsedEvents.sparkSession.implicits._

    val missingContracts = context.callsiteContext.enrichContext("Find missing contracts") {
      parsedEvents
        .select("contractAddress")
        .as[String]
        .map(_.toLowerCase)
        .distinct
        .except(F.broadcast(metadataDs).select("address").as[String].map(_.toLowerCase))
        .collect()
        .toSeq
    }

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
        } yield n.nonEmpty && s.nonEmpty

        symbolAndDecimalsAreOk.getOrElse(false)
      })
      .write
      .mode(SaveMode.Append)
      .parquet(config.metadataCachePath)

    metadataDs.union(newMetadataDs)
  }

  def getTokenMetadata(ttx: Dataset[TokenTransactionRawValue],
                       config: BlockSummaryPipelineConfiguration)
                      (implicit ec: ExecutionContext, context: Context): Dataset[TokenMetadata] = {
    val updatedMetadata = context.callsiteContext.enrichContext("Load and scrape token metadata") {
      val metadataDs = spark
        .read
        .schema(TokenMetadata.encoder.schema)
        .parquet(config.metadataCachePath)
        .as[TokenMetadata]
      scrapeMissingMetadata(config, ttx, metadataDs)
    }
    config.metadataOutputPath
      .foreach(key =>
        context.callsiteContext.enrichContext("Store token metadata") {
          datasetStore.storeParquet(key.inbox,
            updatedMetadata.filter((metadata: TokenMetadata) => metadata.symbol.exists(!_.isEmpty)))
        }
      )
    updatedMetadata
  }
}

object BlockSummaryPipeline {
  val decimalPrecisionForValues: Int = 3
}

trait BlockSummaryPipelineComponent {
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
  final lazy val driver: BlockSummaryPipeline = new BlockSummaryPipeline(scraper)
}