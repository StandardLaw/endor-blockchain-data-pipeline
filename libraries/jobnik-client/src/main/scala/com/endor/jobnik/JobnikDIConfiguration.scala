package com.endor.jobnik

import com.endor.artifacts.{ArtifactPublishingMode, PublicationMedium, RedisMode}

trait JobnikDIConfiguration {
  def artifactPublishers: ArtifactPublishingMode
  def publicationMedium: PublicationMedium
  def redisMode: Option[RedisMode]
}
