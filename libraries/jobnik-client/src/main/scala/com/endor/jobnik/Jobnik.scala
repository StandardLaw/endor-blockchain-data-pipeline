package com.endor.jobnik

import com.endor.artifacts.{ExceptionArtifact, RedisMode, TopicPublisher}
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.ILoggerFactory
import play.api.libs.json._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

sealed abstract class JobnikMessage(val `type`: String, val jobToken: JsObject, val publishDate: DateTime)

final case class DriverCompletionMessage(override val jobToken: JsObject)
  extends JobnikMessage("driver_completion", jobToken, DateTime.now(DateTimeZone.UTC))

object DriverCompletionMessage {
  @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
  implicit def driverCompletionMessageWriter: Writes[DriverCompletionMessage] =
    (driverCompletionMessage: DriverCompletionMessage) => JsObject(Map[String, JsValue](
      "job_token" -> driverCompletionMessage.jobToken,
      "publish_date" -> JsString(driverCompletionMessage.publishDate.toDateTimeISO.toString),
      "type" -> JsString(driverCompletionMessage.`type`)
    ))
}

final case class ProgressIndicationMessage(override val jobToken: JsObject, identifier: String,
                                           totalTaskCount: Int, returnCode : JobnikReturnCode)

  extends JobnikMessage("progress_indication", jobToken, DateTime.now(DateTimeZone.UTC))

object ProgressIndicationMessage {
  @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
  implicit def progressIndicationMessageWriter: Writes[ProgressIndicationMessage] =
    (progressIndicationMessage: ProgressIndicationMessage) => JsObject(Map[String, JsValue](
      "job_token" -> progressIndicationMessage.jobToken,
      "publish_date" -> JsString(progressIndicationMessage.publishDate.toDateTimeISO.toString),
      "type" -> JsString(progressIndicationMessage.`type`),
      "identifier" -> JsString(progressIndicationMessage.identifier),
      "return_code_name" -> JsString(progressIndicationMessage.returnCode.jobnikReturnCodeName),
      "total_task_count" -> JsNumber(progressIndicationMessage.totalTaskCount)
    ))
}

sealed trait JobnikReturnCode {
  def jobnikReturnCodeName : String
}

object JobnikReturnCode {
  case object Ok extends JobnikReturnCode {
    override val jobnikReturnCodeName: String = "Ok"
  }
  case object ApplicativeError extends JobnikReturnCode {
    override val jobnikReturnCodeName: String = "ApplicativeError"
  }
  case object UnknownError extends JobnikReturnCode {
    override val jobnikReturnCodeName: String = "UnknownError"
  }
}

trait JobnikUnretyableException {
  this: Exception =>
}

class JobnikCommunicator(val pubsubProxy: TopicPublisher) {

  def sendProgressIndication(identifier: String, totalTaskCount: Int,
                             returnCode: JobnikReturnCode = JobnikReturnCode.Ok)
                            (implicit jobnikSession: Option[JobnikSession]): Unit = {
    jobnikSession match {
      case Some(session) =>
        val message = ProgressIndicationMessage(session.jobToken, identifier, totalTaskCount, returnCode)
        tryPublishingMessage(message)
      case None =>
    }

  }

  private def tryPublishingMessage[T <: JobnikMessage](message: T)
                                                      (implicit jobnikSession: Option[JobnikSession],
                                                       writer : Writes[T]): Unit = {
    getTopicName.foreach { topic =>
      pubsubProxy.publish(topic, Json.toJson[T](message).toString)
    }
  }

  def getTopicName(implicit jobnikSession: Option[JobnikSession]): Option[String] = {
    jobnikSession
      .map(session => s"${session.jobnikRole}-jobnik-communication")
  }
}

final case class JobnikContainer(diConfig: JobnikDIConfiguration, loggerFactory: ILoggerFactory)

object Jobnik {
  def monitor[T](identifier: String, totalMonitorCalls: Int, currentCallOrdinal: Int)
                (body: => T)
                (implicit jobnikContainer: JobnikContainer, jobnikSession: Option[JobnikSession]): T = {

    implicit val ec: ExecutionContextExecutor = ExecutionContext.global

    val result = monitorAsync(identifier, totalMonitorCalls, currentCallOrdinal) {
      Future(body)
    }

    Await.result(result, Duration.Inf)
  }

  def monitorAsync[T](identifier: String, totalMonitorCalls: Int, currentCallOrdinal: Int)
                     (body: => Future[T])
                     (implicit jobnikContainer: JobnikContainer, jobnikSession: Option[JobnikSession],
                      ec: ExecutionContext): Future[T] = {

    implicit val loggerContext: ILoggerFactory = jobnikContainer.loggerFactory
    implicit val redisMode: Option[RedisMode] = jobnikContainer.diConfig.redisMode

    val topicPublisher = jobnikContainer.diConfig.publicationMedium.createPublicationAdapter
    val artifactPublisher = jobnikContainer.diConfig.artifactPublishers.createPublisher

    val jobnikCommunicator = new JobnikCommunicator(topicPublisher)

    def handleException(e: Throwable, returnCode: JobnikReturnCode): Throwable = {
      try {
        jobnikContainer.loggerFactory.getLogger(this.getClass.getName).error("An unhandled exception has occurred", e)
      } catch {
        case _: Throwable =>
      }
      try {
        artifactPublisher.publishArtifact(ExceptionArtifact(e))
      } catch {
        case _: Throwable =>
      }
      // currentCallOrdinal is the amount of actual calls to monitor that can be expected in case of failure,
      // since if this call failed there would be no further calls to monitor in this context
      jobnikCommunicator.sendProgressIndication(identifier, totalMonitorCalls * 2, returnCode)
      (1 to (totalMonitorCalls - currentCallOrdinal) * 2)
        .foreach(i => jobnikCommunicator.sendProgressIndication(s"dummyFail-$i", totalMonitorCalls * 2, returnCode))
      e
    }

    jobnikCommunicator.sendProgressIndication(s"$identifier-start", totalMonitorCalls * 2)
    body
      .transform(
        result => {
          jobnikCommunicator.sendProgressIndication(s"$identifier-end", totalMonitorCalls * 2)
          result
        },
        {
          case e: JobnikUnretyableException =>
            handleException(e, JobnikReturnCode.ApplicativeError)
          case e: Throwable =>
            handleException(e, JobnikReturnCode.UnknownError)

        })
  }
}
