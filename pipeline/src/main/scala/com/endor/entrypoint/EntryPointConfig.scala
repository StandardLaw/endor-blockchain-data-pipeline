package com.endor.entrypoint

import play.api.libs.json._

/**
  * Created by izik on 19/06/2016.
  */
object EntryPointConfig {
  implicit lazy val format: OFormat[EntryPointConfig] = Json.format[EntryPointConfig]
}

final case class EntryPointConfig(operation: String, customer: String, executionId: String,
                                  subIdentifiers: Map[String, String] = Map())
