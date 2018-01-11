package com.endor.artifacts

import com.endor.jobnik.{JobnikSession, MessageMonitor}

/**
  * Created by izik on 09/01/2017.
  */
class RedisMessageMonitor(implicit redisMode: Option[RedisMode]) extends MessageMonitor {
  override def notifyMessageSent(techName: String, messageId: String)
                                (implicit jobnikSession: Option[JobnikSession]): Unit = {
    jobnikSession match {
      case Some(session@JobnikSession(jobnikRole, jobToken)) =>
        val jobId = jobToken.value("jobId").as[String]
        val setKey = s"$jobnikRole-$jobId-$techName-artifacts-producer"
        val maybeRedisMessageLog = redisMode.map(_.messageLog(session))

        Retries.retryWithExponentialBackoff(3) {
          // If we don't have redis, do not add the key to the set because we're probably not publishing artifacts
          maybeRedisMessageLog.foreach(_.sadd(setKey, messageId))
        }
      case None =>
    }
  }
}
