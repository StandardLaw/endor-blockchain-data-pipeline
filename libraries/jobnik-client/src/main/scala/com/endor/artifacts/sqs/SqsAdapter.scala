package com.endor.artifacts.sqs

import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.endor.artifacts.{Retries, TopicPublisher}
import org.slf4j.{ILoggerFactory, MDC}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class SqsAdapter(implicit ec: ExecutionContext, loggingFactory: ILoggerFactory) extends TopicPublisher {
  val sqsClient: AmazonSQS = AmazonSQSClientBuilder.defaultClient()

  private lazy val logger = loggingFactory.getLogger(this.getClass.getName)
  private val queueUrls : mutable.Map[String, String] = mutable.Map()

  private def withTopicMdc(topic: String)(log: => Unit): Unit = {
    MDC.put("topic", topic)
    try {
      log
    } finally {
      MDC.remove("topic")
    }
  }

  override def publish(topicLogicalName: String, messageBody: String): Unit = {
    if (messageBody.length >= 256 * 1024) {
      logger.warn(s"Attempting to publish message which is larger than 256KB to SQS :(")
    }

    def onPublicationAttemptFailure(attemptNumber: Int): Unit =
      withTopicMdc(topicLogicalName) {
        logger.warn(s"Failed attempt $attemptNumber to publish artifact. Will retry. msg: $messageBody")
      }

    val onPublicationFailure: () => Unit = () => {
      withTopicMdc(topicLogicalName) {
        logger.error(s"Failed to publish artifact. msg: $messageBody")
      }
    }

    val retry = Retries.retryWithExponentialBackoff[Unit](
      5,
      onAttemptFailure = onPublicationAttemptFailure,
      onFailure = onPublicationFailure
    )(_)

    retry {
      val eventualSendMessageResult = Future { sqsClient.sendMessage(getQueueUrl(topicLogicalName), messageBody) }
      Await.result(eventualSendMessageResult, 10 seconds)
    }

  }

  private def getQueueUrl(queueName : String) : String = {
    queueUrls.getOrElseUpdate(queueName, {
      Await.result(Future { sqsClient.getQueueUrl(queueName) }.map(_.getQueueUrl), 10 seconds)
    })
  }
}
