package com.endor.jobnik

import play.api.libs.json.{JsObject, Json, OFormat}

/**
  * Created by izik on 5/11/16.
  */
final case class JobnikSession(jobnikRole: String, jobToken: JsObject)

object JobnikSession {
  implicit lazy val format: OFormat[JobnikSession] = Json.format[JobnikSession]
}