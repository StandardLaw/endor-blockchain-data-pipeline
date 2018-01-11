package com.endor.jobnik

import com.endor.artifacts._
import org.joda.time.DateTime
import org.slf4j.ILoggerFactory
import play.api.libs.json.{JsObject, JsString, Json, Writes}

/**
  * Created by izik on 09/01/2017.
  */
object ArtifactWrapper {
  @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
  implicit def artifactWrapperWriter[T <: Artifact](implicit fmt: Writes[T]): Writes[ArtifactWrapper[T]] =
    (artifactWrapper: ArtifactWrapper[T]) => JsObject(Seq(
      "job_token" -> artifactWrapper.jobToken,
      "publish_date" -> JsString(artifactWrapper.publishDate.toDateTimeISO.toString),
      "type" -> JsString(artifactWrapper.`type`),
      "messageId" -> JsString(artifactWrapper.messageId),
      "artifact" -> fmt.writes(artifactWrapper.artifact)
    ))
}

final case class ArtifactWrapper[T <: Artifact](jobToken : JsObject, publishDate : DateTime, `type` : String, artifact : T) {
  val messageId: String = java.util.UUID.randomUUID.toString
}

trait MessageMonitor {
  def notifyMessageSent(techName : String, messageId : String)(implicit jobnikSession : Option[JobnikSession]): Unit
}

class JobnikArtifactPublisher(messageMonitor : MessageMonitor, publishers : Map[String, TopicPublisher])
                             (implicit loggingFactory: ILoggerFactory) extends ArtifactPublisher {
  private lazy val logger = loggingFactory.getLogger(this.getClass.getName)

  override def publishArtifact[T <: Artifact](artifact: T)(implicit serializer: Writes[T],
                                                           jobnikSession : Option[JobnikSession]): Unit = {
    jobnikSession.foreach {
      case JobnikSession(jobnikRole, jobToken) =>
        val wrapper = ArtifactWrapper(jobToken, DateTime.now(), artifact.artifactType, artifact)
        messageMonitor.notifyMessageSent("general", wrapper.messageId)
        publishers.foreach {
          case (publisherName, publisher) =>
            Retries.retryWithExponentialBackoff(3, onFailure = () => logger.error(s"Failed to publish artifact of type ${wrapper.`type`}")) {
              publisher.publish(s"$jobnikRole-artifacts-eventbus", Json.toJson(wrapper).toString())
            }
            messageMonitor.notifyMessageSent(publisherName, wrapper.messageId)
        }

    }
  }
}











