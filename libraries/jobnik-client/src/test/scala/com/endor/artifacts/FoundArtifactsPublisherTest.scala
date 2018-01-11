package com.endor.artifacts

import com.endor.artifacts.elasticsearch.ElasticsearchProxy
import com.endor.jobnik.JobnikSession
import org.scalatest.FunSuite
import play.api.libs.json._


// used to test external integration with found.
class FoundArtifactsPublisherTest extends FunSuite {
  private implicit val petWrites: OWrites[Pet] = Json.writes[Pet]
  private final case class Pet(name: String, kind : String)

  private implicit val personWrites: OWrites[Person] = Json.writes[Person]
  private final case class Person(name : String, age : Int) extends Artifact { val artifactType: String = "person"}

  private implicit val person2Writes: OWrites[Person2] = Json.writes[Person2]
  private final case class Person2(name : String, age : Int, pet : Option[Pet]) extends Artifact { val artifactType: String = "person2"}

  private implicit val jobnikSession = Option(JobnikSession("izikRole", JsObject(Map[String, JsValue](
    "aaa" -> JsNumber(1),
    "bbb" -> JsString("asd")
  ))))

  ignore("able to store artifacts to real found cluster") {
    val client = new FoundArtifactsPublisher(ElasticsearchProxy.foundProxy, indexPrefix = "aaa")
    client.publishArtifact(Person("izik", 34))

  }

  ignore("able to store artifacts to real found cluster with optional datatype") {
    val client = new FoundArtifactsPublisher(ElasticsearchProxy.foundProxy, indexPrefix = "aaa")
    client.publishArtifact(Person2("izik", 34, None))
    client.publishArtifact(Person2("izik", 34, Option(Pet("blaki-joe", "dog"))))
  }
}
