package com.endor.blockchain.ethereum.blocksummaries

import java.io.File
import java.nio.file.Files
import java.sql.DriverManager

import com.endor.infra.spark.SparkDriverSuite
import com.endor.infra.{BaseComponent, DIConfiguration, LoggingComponent}
import com.endor.{CustomerId, DataId, DataKey}
import com.endor.storage.sources._
import com.wix.mysql.EmbeddedMysql
import com.wix.mysql.EmbeddedMysql.anEmbeddedMysql
import com.wix.mysql.ScriptResolver.classPathScript
import com.wix.mysql.config.DownloadConfig.aDownloadConfig
import com.wix.mysql.distribution.Version.v5_7_latest
import org.apache.spark.sql.{SparkSession, functions => F}
import org.scalatest.{Outcome, fixture}

import scala.concurrent.ExecutionContext.Implicits._

trait BlockSummaryPipelineTestComponent extends BlockSummaryPipelineComponent
  with LoggingComponent with BaseComponent {
  override def diConfiguration: DIConfiguration = DIConfiguration.ALL_IN_MEM
}

class BlockSummaryPipelineTest extends fixture.FunSuite with SparkDriverSuite {
  private def getConfig(fixture: FixtureParam): BlockSummaryPipelineConfiguration = {
    val mysqlConfig = fixture.mysqld.getConfig
    BlockSummaryPipelineConfiguration(
      DatabaseConfig("ethereum", "localhost", mysqlConfig.getUsername, mysqlConfig.getPassword, mysqlConfig.getPort),
      DataKey(CustomerId("testCustomer"), DataId("tx")),
      DataKey(CustomerId("testCustomer"), DataId("ttx")),
      DataKey(CustomerId("testCustomer"), DataId("rewards")),
      fixture.metadataPath, None
    )
  }

  def loadSummary(blockNumber: Long, mysqld: EmbeddedMysql): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val config = mysqld.getConfig
    // Setup the connection with the DB
    val connection =
      DriverManager.getConnection(s"jdbc:mysql://${config.getUsername}:${config.getPassword}@localhost:${config.getPort}/ethereum")
    val dataStream = getClass.getResource(s"/com/endor/blockchain/ethereum/db/summaries/summary-$blockNumber.bin").openStream()
    val preparedStatement = connection
      .prepareStatement("insert into summaries values (?, ?, ?)")
    preparedStatement.setBlob(1, dataStream)
    preparedStatement.setLong(2, blockNumber)
    preparedStatement.setLong(3, blockNumber)
    preparedStatement.execute()
  }

  override protected def withFixture(test: OneArgTest): Outcome = {
    val mysqld = {
      val downloadConfig = aDownloadConfig()
        .withCacheDir(System.getProperty("java.io.tmpdir"))
        .build()

      anEmbeddedMysql(v5_7_latest, downloadConfig)
        .addSchema("ethereum", classPathScript("/com/endor/blockchain/ethereum/db/001_init.sql"))
        .start()
    }
    val metadataPath = createTempDir(randomString(10))
    Files.createDirectory(new File(metadataPath).toPath)
    val metadataStream = getClass.getResource(s"/metadata/snapshot-1.parquet").getPath
    Files.copy(new File(metadataStream).toPath, new File(s"$metadataPath/data.parquet").toPath)
    try {
      withFixture(test.toNoArgTest(FixtureParam(mysqld, metadataPath)))
    } finally {
      mysqld.stop()
    }
  }
  case class FixtureParam(mysqld: EmbeddedMysql, metadataPath: String)

  private def createContainer(): BlockSummaryPipelineTestComponent = new BlockSummaryPipelineTestComponent {
    override implicit def spark: SparkSession = BlockSummaryPipelineTest.this.spark
  }

  test("Will only load new blocks") { fixture =>
    val session = spark
    import session.implicits._
    val container = createContainer()
    loadSummary(46147, fixture.mysqld)
    val config = getConfig(fixture)
    container.datasetStore.storeParquet(config.transactionsOutput.onBoarded, spark.emptyDataset[Transaction])
    container.driver.run(config)
    val loadedData = container.datasetStore.loadParquet(config.transactionsOutput.inbox)
    container.datasetStore.delete(config.transactionsOutput.inbox)
    container.datasetStore.storeParquet(config.transactionsOutput.onBoarded, loadedData)
    loadSummary(46169, fixture.mysqld)
    container.driver.run(config)
    val newLoaded = container.datasetStore.loadParquet(config.transactionsOutput.inbox)
    newLoaded.select(F.col("blockNumber").as[Long]).distinct().collect() should contain only 46169
  }
}
