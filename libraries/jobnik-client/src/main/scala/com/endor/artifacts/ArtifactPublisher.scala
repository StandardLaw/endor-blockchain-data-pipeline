package com.endor.artifacts

import com.endor.artifacts.sqs.SqsAdapter
import com.endor.jobnik.{JobnikArtifactPublisher, JobnikSession}
import org.slf4j.ILoggerFactory
import play.api.libs.json.Writes

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.util.control.NonFatal

/**
  * Created by izik on 5/9/16.
  */
trait Artifact {
  def artifactType : String
}

trait ArtifactPublisher {
  def isRequired : Boolean = true
  def publishArtifact[T <: Artifact : Writes](artifact : T)(implicit jobnikSession: Option[JobnikSession]): Unit
}

class InMemoryArtifactPublisher extends ArtifactPublisher {
  val publishedArtifacts: mutable.MutableList[Artifact] = mutable.MutableList[Artifact]()

  override def publishArtifact[T <: Artifact : Writes](artifact: T)
                                                      (implicit jobnikSession: Option[JobnikSession]): Unit = {
    publishedArtifacts += artifact
  }
}

class CompositeArtifactPublisher(implicit loggerContext: ILoggerFactory, redisMode: Option[RedisMode])
  extends ArtifactPublisher {

  private val publishers: Seq[ArtifactPublisher] = {
    implicit val ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global

    Seq(
      new JobnikArtifactPublisher(new RedisMessageMonitor,
        Map(
          "sqs" -> new SqsAdapter
        )
      )
    )
  }

  private lazy val logger = loggerContext.getLogger(this.getClass.getName)

  override def publishArtifact[T <: Artifact : Writes](artifact: T)
                                                      (implicit jobnikSession: Option[JobnikSession]): Unit = {
    publishers.foreach(publisher => {
      try {
        publisher.publishArtifact(artifact)
      } catch {
        case NonFatal(ex) if publisher.isRequired =>
          logger.error(s"failed to publish artifacts to required publisher (${publisher.getClass.getName}).", ex)
          throw ex
        case NonFatal(ex) =>
          logger.warn(s"failed to publish artifacts to non-required publisher (${publisher.getClass.getName}).", ex)
      }
    })
  }
}