package com.endor.artifacts

import com.endor.serialization._
import org.slf4j.ILoggerFactory
import play.api.libs.json.OFormat

/**
  * Created by user on 04/05/17.
  */
sealed trait ArtifactPublishingMode {
  def createPublisher()(implicit loggerContext: ILoggerFactory, redisMode: Option[RedisMode]): ArtifactPublisher
}

object ArtifactPublishingMode {
  case object InMemory extends ArtifactPublishingMode {
    override def createPublisher()(implicit loggerContext: ILoggerFactory, redisMode: Option[RedisMode]): ArtifactPublisher =
      new InMemoryArtifactPublisher()
  }

  case object Real extends ArtifactPublishingMode {
    override def createPublisher()(implicit loggerContext: ILoggerFactory, redisMode: Option[RedisMode]): ArtifactPublisher =
      new CompositeArtifactPublisher()
  }

  implicit val format: OFormat[ArtifactPublishingMode] = formatFor(
    "InMemory" -> InMemory,
    "Real" -> Real
  )
}
