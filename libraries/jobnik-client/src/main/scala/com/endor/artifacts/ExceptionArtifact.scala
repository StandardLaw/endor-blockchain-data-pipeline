package com.endor.artifacts

import java.io.{PrintWriter, StringWriter}

import play.api.libs.json.{Json, OFormat}

/**
  * Created by izik on 19/06/2016.
  */
object ExceptionArtifact {
  implicit lazy val format: OFormat[ExceptionArtifact] = Json.format[ExceptionArtifact]

  def apply(e: Throwable): ExceptionArtifact = {
    val stackTrace = {
      val sw = new StringWriter
      e.printStackTrace(new PrintWriter(sw))
      sw.toString
    }


    ExceptionArtifact(e.getMessage, stackTrace)
  }
}

final case class ExceptionArtifact(msg : String, stackTrace : String)
  extends Artifact {

  override def artifactType: String = "exception"
}