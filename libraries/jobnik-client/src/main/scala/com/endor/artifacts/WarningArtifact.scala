package com.endor.artifacts

import play.api.libs.json.{Json, OFormat}

/**
  * Created by izik on 19/06/2016.
  */

trait KnownWarnings {
  def asArtifact() : WarningArtifact
}
object KnownWarnings {
  case object NoInputCsvCouldBeCreated extends KnownWarnings {
    override def asArtifact(): WarningArtifact = WarningArtifact(1000, "No input csv could be created.")
  }
  final case class SomeCsvCouldBeCreated(sliceSize : Int) extends KnownWarnings {
    override def asArtifact(): WarningArtifact = WarningArtifact(1001, s"input csv for slice $sliceSize could not be created.")
  }
  case object EmptyOnboarding extends KnownWarnings {
    override def asArtifact(): WarningArtifact = WarningArtifact(1002, "on boarding empty batch.")
  }


}

object WarningArtifact {
  implicit lazy val format: OFormat[WarningArtifact] = Json.format[WarningArtifact]
}

final case class WarningArtifact(errorCode: Int, msg : String)
  extends Artifact {

  override def artifactType: String = "warningArtifact"
}