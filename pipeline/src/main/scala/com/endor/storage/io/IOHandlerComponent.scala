package com.endor.storage.io

import com.endor.infra.DIConfigurationComponent
import com.endor.serialization._
import play.api.libs.json.{Json, OFormat}

import scala.collection.mutable

trait IOHandlerComponent {
  this: DIConfigurationComponent =>

  private lazy val handlers = mutable.Map[IoHandlerType, IOHandler]()

  def ioHandler(ioHandlerType: IoHandlerType) : IOHandler = {
    handlers.getOrElseUpdate(ioHandlerType, buildHandler(ioHandlerType))
  }

  private def buildHandler(ioHandlerType: IoHandlerType) : IOHandler = {
    ioHandlerType match {
      case IoHandlerType.InMemory => new InMemoryIOHandler()
      case IoHandlerType.S3 => new S3IOHandler(diConfiguration.sparkInfrastructure)
      case IoHandlerType.LocalFs(Some(baseDir)) => new LocalIOHandler(baseDir)
      case IoHandlerType.LocalFs(None) => new LocalIOHandler()
    }
  }

  implicit lazy val dataFrameIOHandler: IOHandler = ioHandler(diConfiguration.dataFrameSource)
}

sealed trait IoHandlerType

object IoHandlerType {
  case object InMemory extends IoHandlerType
  case object S3 extends IoHandlerType
  final case class LocalFs(baseDir: Option[String] = None) extends IoHandlerType

  implicit val format: OFormat[IoHandlerType] = formatFor(
    "InMemory" -> InMemory,
    "S3" -> S3,
    "LocalFs" -> Json.format[LocalFs]
  )
}