package com.endor.infra

import com.endor.artifacts.{ArtifactPublishingMode, PublicationMedium, RedisMode}
import com.endor.infra.spark.SparkInfrastructure
import com.endor.jobnik.JobnikDIConfiguration
import com.endor.storage.io.IoHandlerType
import play.api.libs.json.{Json, OFormat}

final case class DIConfiguration(artifactPublishers: ArtifactPublishingMode,
                                 dataFrameSource: IoHandlerType,
                                 sparkInfrastructure: SparkInfrastructure,
                                 publicationMedium: PublicationMedium,
                                 redisMode: Option[RedisMode]) extends JobnikDIConfiguration

object DIConfiguration {
  implicit lazy val format: OFormat[DIConfiguration] = Json.format[DIConfiguration]

  val PROD: DIConfiguration = DIConfiguration(
    artifactPublishers = ArtifactPublishingMode.Real,
    dataFrameSource = IoHandlerType.S3,
    sparkInfrastructure = SparkInfrastructure.EMR(true),
    publicationMedium = PublicationMedium.AmazonSQS,
    redisMode = Option(RedisMode.Real)
  )

  val ALL_IN_MEM: DIConfiguration = DIConfiguration(
    artifactPublishers = ArtifactPublishingMode.InMemory,
    dataFrameSource = IoHandlerType.InMemory,
    sparkInfrastructure = SparkInfrastructure.Local(true),
    publicationMedium = PublicationMedium.InMemory,
    redisMode = None
  )
}
