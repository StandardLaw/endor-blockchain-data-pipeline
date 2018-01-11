package com.endor.artifacts

/**
  * Created by izik on 06/03/2017.
  */
trait TopicPublisher {
  def publish(topicLogicalName : String, messageBody : String) : Unit
}
