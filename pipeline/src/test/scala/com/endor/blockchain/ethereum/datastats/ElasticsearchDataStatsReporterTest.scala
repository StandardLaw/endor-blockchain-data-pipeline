package com.endor.blockchain.ethereum.datastats

import java.sql.Timestamp

import com.endor.blockchain.ethereum.transaction.ProcessedTransaction
import com.endor.infra.spark.SparkDriverFunSuite
import com.endor.infra.{BaseComponent, DIConfiguration}
import com.endor.storage.sources._
import com.endor.{CustomerId, DataId, DataKey}
import com.sksamuel.elastic4s.embedded.LocalNode
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{HttpClient, Response}
import org.apache.spark.sql.SparkSession
import play.api.libs.json.{Json, Reads}

trait ElasticsearchDataStatsReporterTestComponent extends ElasticsearchDataStatsReporterComponent with BaseComponent {
  override val diConfiguration: DIConfiguration = DIConfiguration.ALL_IN_MEM
}

class ElasticsearchDataStatsReporterTest extends SparkDriverFunSuite {
  case class Node(client: HttpClient, ip: String, port: String)

  private def clientResponseToObjects[T: Reads](response: Response[SearchResponse]): Seq[T] = {
    response.result.hits.hits.map(_.sourceAsString).map(Json.parse).map(_.as[T])
  }

  private def createNode(): Node = {
    val localNode = LocalNode("testcluster", createTempDir(randomString(10)))
    val client: HttpClient = localNode.http(true)
    val Array(ip, port) = localNode.ipAndPort.split(":")
    Node(client, ip, port)
  }

  private def createContainer(): ElasticsearchDataStatsReporterTestComponent =
    new ElasticsearchDataStatsReporterTestComponent {
      override implicit def spark: SparkSession = ElasticsearchDataStatsReporterTest.this.spark
    }

  test("basic single block test") {
    val sess = spark
    import sess.implicits._
    val container = createContainer()
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    val Node(client, ip, port) = createNode()
    val dataKey = DataKey[ProcessedTransaction](CustomerId("testCustomer"), DataId("testDs") )
    val config = ElasticsearchDataStatsConfig(dataKey, "test1", ip, port.toInt)
    val inputPath = this.getClass.getResource("/com/endor/blockchain/ethereum/blocks/parsed/multi.parquet").toString
    val inputDs = spark.read.parquet(inputPath).as[ProcessedTransaction]
    container.datasetStore.storeParquet(dataKey.onBoarded, inputDs)
    container.driver.run(config = config)
    val ts = new Timestamp(0)
    val selfComputed = inputDs
      .groupByKey(_.blockNumber)
      .mapGroups {
        (blockNumber, txIt) =>
          val a = txIt.foldLeft(BlockStats(blockNumber, ts, 0, Set.empty)) {
            case (acc, tx) => acc.copy(numTx = acc.numTx + 1, timestamp = tx.timestamp, addresses = acc.addresses + tx.sendAddress)
          }
          a.copy(numTx = a.numTx / 2)
      }
    val expected = selfComputed.collect()
    val resp = client.execute {
      searchWithType(config.elasticsearchIndex / BlockStats.esType) limit expected.length + 1
    }.await
    resp.isRight should be (true)
    resp match {
      case Right(success) =>
        val results = clientResponseToObjects[BlockStats](success)
        println(results)
        results should contain theSameElementsAs expected
      case _ =>
    }
  }
}
