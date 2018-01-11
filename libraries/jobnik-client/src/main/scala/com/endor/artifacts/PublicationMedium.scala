package com.endor.artifacts

import com.endor.artifacts.sqs.SqsAdapter
import com.endor.serialization.formatFor
import org.slf4j.ILoggerFactory
import play.api.libs.json.OFormat

import scala.concurrent.ExecutionContextExecutor

/**
  * Created by user on 15/05/17.
  */
sealed trait PublicationMedium {
  def createPublicationAdapter()(implicit loggerFactory: ILoggerFactory): TopicPublisher
}
object PublicationMedium {
  case object AmazonSQS extends PublicationMedium {
    override def createPublicationAdapter()(implicit loggerFactory: ILoggerFactory): TopicPublisher = {
      implicit val ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
      new SqsAdapter
    }
  }
  case object InMemory extends PublicationMedium {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var topicPublisher: Option[InMemoryTopicPublisher] = None
    override def createPublicationAdapter()(implicit loggerFactory: ILoggerFactory): TopicPublisher = {
      topicPublisher.getOrElse {
        val publisher = new InMemoryTopicPublisher()
        topicPublisher = Option(publisher)
        publisher
      }
    }
  }

  implicit val format: OFormat[PublicationMedium] = formatFor(
    "AmazonSQS" -> AmazonSQS,
    "InMemory" -> InMemory
  )
}
