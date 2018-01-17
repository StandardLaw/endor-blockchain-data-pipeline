package com.endor.blockchain.ethereum.tokens

import akka.actor.ActorSystem
import com.endor.context.Context
import com.endor.entrypoint.EntryPointConfig
import com.endor.infra.spark.{SparkApplication, SparkEntryPointConfiguration, SparkInfrastructure}
import com.endor.jobnik.JobnikSession
import com.endor.spark.blockchain.ethereum.token.metadata._
import com.endor.storage.io.S3IOHandler
import org.apache.spark.sql.SparkSession
import org.web3j.protocol.Web3j
import org.web3j.protocol.http.HttpService

import scala.concurrent.ExecutionContext

object EMRTokensPipeline extends SparkApplication[EthereumTokensPipelineConfig] {
  override protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[EthereumTokensPipelineConfig]): EntryPointConfig =
    EntryPointConfig("EthereumBlocksToTokenTransactions")


  override protected def run(sparkSession: SparkSession, configuration: EthereumTokensPipelineConfig)
                            (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit = {
    val scraper = {
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
    val ioHandler = new S3IOHandler(SparkInfrastructure.EMR(true))
    val driver = new EthereumTokensPipeline(scraper, ioHandler)(sparkSession)
    implicit val ec: ExecutionContext = ExecutionContext.global
    driver.run(configuration)
  }
}