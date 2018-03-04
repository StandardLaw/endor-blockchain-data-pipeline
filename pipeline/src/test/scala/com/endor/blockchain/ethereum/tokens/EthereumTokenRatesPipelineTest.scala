package com.endor.blockchain.ethereum.tokens

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit

import com.endor._
import com.endor.infra.spark.SparkDriverFunSuite
import com.endor.infra.{BaseComponent, DIConfiguration}
import com.endor.storage.sources._
import org.apache.spark.sql.SparkSession

trait EthereumTokenRatesPipelineTestComponent extends EthereumTokenRatesPipelineComponent with BaseComponent {
  override val diConfiguration: DIConfiguration = DIConfiguration.ALL_IN_MEM
}

class EthereumTokenRatesPipelineTest extends SparkDriverFunSuite {
  private def createContainer(tokenList: Seq[String]): EthereumTokenRatesPipelineTestComponent = new EthereumTokenRatesPipelineTestComponent {
    override implicit def spark: SparkSession = EthereumTokenRatesPipelineTest.this.spark

    @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
    override def tokenListScraper: EthereumTokensOps.TokenListScraper = () => tokenList
  }

  test("Basic test") {
    val sparkSession = spark
    import sparkSession.implicits._
    val tokens = (0 to 10).map(_ =>
      EthereumTokensOps.normalizeName(randomString(10)) -> EthereumTokensOps.normalizeName(randomString(3)))
    val container = createContainer(tokens.map(_._1))
    val rawData = createDataForTokens(tokens, Instant.now())
    val inputPath = createTempDir(randomString(10))
    spark
      .createDataset(rawData)
      .write
      .json(inputPath)
    val metadata = tokens
      .map {
        case (name, symbol) => name -> MetadataRow(name, symbol, s"0x${randomString(20)}")
      }
      .toMap
    val metadataPath = createTempDir(randomString(10))
    spark
      .createDataset(metadata.values.toSeq)
      .write
      .parquet(metadataPath)
    val config = EthereumTokenRatesPipelineConfig(
      inputPath,
      metadataPath,
      DataKey[RateRow](CustomerId("my-customer"), DataId("my-dataset"))
    )
    container.dataFrameIOHandler.saveRawData("someData", inputPath)
    container.driver.run(config)
    val result = container.datasetStore.loadParquet(config.output.inbox).collect()

    val expectedResult = rawData
      .flatMap(rawRow => {
        val currentMetadata = metadata.get(rawRow.name)
        rawRow.price_usd.map(price => RateRow(rawRow.name, rawRow.symbol, price.toDouble,
          rawRow.market_cap_usd.map(_.toDouble),
          currentMetadata.map(_.name),
          currentMetadata.map(_.symbol),
          currentMetadata.map(_.address),
          Timestamp.from(Instant.ofEpochSecond(rawRow.last_updated.toLong))))
      })

    result should contain theSameElementsAs expectedResult
  }

  private def createDataForTokens(tokens: Seq[(String, String)], firstTs: Instant) = {
    tokens
      .flatMap {
        case (name, symbol) =>
          val basePrice = randomGenerator.nextDouble() * 10
          val marketCap = randomGenerator.nextInt(1000000)
          (0 to randomGenerator.nextInt(15))
            .map(i => RawRateRow(name, symbol, Option(basePrice + randomGenerator.nextDouble()).map(_.toString),
              Option(marketCap.toString), firstTs.plus(i * 5L, ChronoUnit.MINUTES).getEpochSecond.toString))
      }
  }
}
