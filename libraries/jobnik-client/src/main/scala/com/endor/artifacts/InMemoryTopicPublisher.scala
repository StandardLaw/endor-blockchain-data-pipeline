package com.endor.artifacts

import scala.collection.mutable

/**
  * Created by user on 13/03/17.
  */
class InMemoryTopicPublisher() extends TopicPublisher {
  val topicsToMessages: mutable.Map[String, mutable.ListBuffer[String]] = mutable.Map.empty

  override def publish(topicLogicalName: String, messageBody: String): Unit = {
    topicsToMessages.getOrElseUpdate(topicLogicalName, mutable.ListBuffer.empty[String]) += messageBody
  }
}
