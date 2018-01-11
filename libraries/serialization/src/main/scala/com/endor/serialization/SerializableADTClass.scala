package com.endor.serialization

import play.api.libs.json._

import scala.reflect.ClassTag

/**
  * Created by user on 01/12/16.
  */
final case class SerializableADTClass[Parent, T <: Parent : ClassTag](typeName: String, format: OFormat[T])
  extends SerializableADT[Parent] {

  val reads: PartialFunction[JsValue, JsResult[Parent]] = {
    case js: JsObject if (js \ "typeName").as[String] == typeName =>
      format.reads(js)
  }

  val writes: PartialFunction[Parent, JsObject] = {
    case obj: T =>
      format.writes(obj) + ("typeName" -> JsString(typeName))
  }
}