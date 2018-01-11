package com.endor.artifacts

import com.endor.jobnik.JobnikSession
import com.endor.serialization._
import com.redis.RedisClient
import play.api.libs.json._

sealed trait RedisMode {
  def messageLog(implicit jobnikSession: JobnikSession): RedisClient
  def tasks(implicit jobnikSession: JobnikSession): RedisClient
}

object RedisMode {
  case object Local extends RedisMode {
    private def redis: RedisClient = new RedisClient()

    def messageLog(implicit jobnikSession: JobnikSession): RedisClient = redis
    def tasks(implicit jobnikSession: JobnikSession): RedisClient = redis
  }

  case object Real extends RedisMode {
    def messageLog(implicit jobnikSession: JobnikSession): RedisClient =
      new RedisClient(s"message-log.redis.${jobnikSession.jobnikRole}.endorians.com", 6379)

    def tasks(implicit jobnikSession: JobnikSession): RedisClient =
      new RedisClient(s"tasks.redis.${jobnikSession.jobnikRole}.endorians.com", 6379)
  }

  implicit lazy val format: Format[RedisMode] = formatFor(
    "Local" -> Local,
    "Real" -> Real
  )
}

