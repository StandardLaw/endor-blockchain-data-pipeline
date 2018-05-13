package com.endor.blockchain.ethereum.transaction

import com.endor.infra.spark.SparkDriverFunSuite
import com.sksamuel.elastic4s.embedded.LocalNode
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{HttpClient, Response}
import org.apache.spark.sql.{Encoder, Encoders}
import org.elasticsearch.spark.sql._
import play.api.libs.json.{Json, OFormat, Reads}

final case class Person(name: String, age:Int)

object Person {
  implicit val format: OFormat[Person] = Json.format
  implicit val encoder: Encoder[Person] = Encoders.product
}

class TestEs extends SparkDriverFunSuite {
  case class Node(client: HttpClient, ip: String, port: String)

  private def clientResponseToObjects[T: Reads](response: Response[SearchResponse]): Seq[T] = {
    response.result.hits.hits.map(_.sourceAsString).map(Json.parse).map(_.as[T])
  }

  def createNode(): Node = {
    val localNode = LocalNode("testcluster", createTempDir(randomString(10)))
    val client: HttpClient = localNode.http(true)
    val Array(ip, port) = localNode.ipAndPort.split(":")
    Node(client, ip, port)
  }

  test("es") {
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    val Node(client, ip, port) = createNode()

    val data = Seq(Person("lior", 25), Person("yuval", 31))
    val ds = spark.createDataset(data)
    ds.saveToEs("lior/test", Map("es.nodes" -> ip, "es.port" -> port))

    val resp = client.execute {
      searchWithType("lior" / "test")
    }.await
    resp.isRight should be (true)
    resp match {
      case Right(success) =>
        val results = clientResponseToObjects[Person](success)
        results should contain theSameElementsAs data
      case _ =>
    }

  }
}
