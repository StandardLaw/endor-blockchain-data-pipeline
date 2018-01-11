package com.endor.serialization

import play.api.libs.json._

/**
  * Created by user on 01/12/16.
  */
trait SerializableADT[T] {
  def reads: PartialFunction[JsValue, JsResult[T]]
  def writes: PartialFunction[T, JsObject]
}
