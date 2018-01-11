package com.endor.artifacts

import com.endor.jobnik.JobnikSession
import org.scalatest.FunSuite
import play.api.libs.json.{Json, OWrites}


// used to test external integration with found.
class JobnikArtifactsPublisherTest extends FunSuite {
  private implicit val petWrites: OWrites[Pet] = Json.writes[Pet]
  private final case class Pet(name: String, kind : String)

  private implicit val personWrites: OWrites[Person] = Json.writes[Person]
  private final case class Person(name : String, age : Int) extends Artifact { val artifactType: String = "person"}

  private implicit val person2Writes: OWrites[Person2] = Json.writes[Person2]
  private final case class Person2(name : String, age : Int, pet : Option[Pet]) extends Artifact { val artifactType: String = "person2"}

  implicit val jobnikSession: Option[JobnikSession] =  Option(
    JobnikSession("izik", Json.obj(
      "a" -> "aaa",
      "b" -> 2,
      "jobId" -> "abcd"
    ))
  )


  def createNewPublisher() : ArtifactPublisher = {
    new InMemoryArtifactPublisher()
  }


  ignore("able to publish artifacts to google pub sub") {
    val client = createNewPublisher()
    client.publishArtifact(Person("izik", 34))
  }


  ignore("able to store artifacts to real found cluster with optional datatype") {
    val client = createNewPublisher()
    client.publishArtifact(Person2("izik", 34, None))
    client.publishArtifact(Person2("izik", 34, Option(Pet("blaki", "dog"))))
  }
}
