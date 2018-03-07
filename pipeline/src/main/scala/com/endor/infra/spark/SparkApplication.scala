package com.endor.infra.spark

import java.util.Base64

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.util.ContextInitializer
import com.endor.context.Context
import com.endor.entrypoint._
import com.endor.infra.DIConfiguration
import com.endor.jobnik.{Jobnik, JobnikContainer, JobnikSession}
import org.apache.spark.sql.SparkSession
import play.api.libs.json._

import scala.util.{Failure, Success, Try}

final case class SparkEntryPointConfiguration[T: Reads](applicationConf: T)

abstract class SparkApplication[T: Reads] {
  protected def createEntryPointConfig(configuration: SparkEntryPointConfiguration[T]): EntryPointConfig
  protected def run(sparkSession: SparkSession, dIConf: DIConfiguration, configuration: T)
                   (implicit context: Context, jobnikSession: Option[JobnikSession]): Unit

  @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
  private def parseConfiguration(configuration: String): SparkEntryPointConfiguration[T] = {
    implicit val reads: Reads[SparkEntryPointConfiguration[T]] = (json: JsValue) => {
      JsSuccess(SparkEntryPointConfiguration(
        (json \ "applicationConf").as[T]
      ))
    }
    parseToJsValue(configuration).as[SparkEntryPointConfiguration[T]]
  }

  private def parseToJsValue(s: String): JsValue = {
    Try(Json.parse(s))
      .orElse(Try(Json.parse(Base64.getDecoder.decode(s).map(_.toChar).mkString("")))) match {
      case Success(result) => result
      case Failure(_) => throw new IllegalArgumentException("Could not parse input as base64 or regular json")
    }
  }

  final def main(args: Array[String]): Unit = {
    args(0) match {
      case "testConfiguration" => println(parseConfiguration(args(3)))
      case "runDriver" => runDriver(args, testMode = false)
      case "debugDriver" => runDriver(args, testMode = true)
    }
  }

  private def runDriver(args: Array[String], testMode: Boolean): Unit = {
    implicit val maybeJobnikSession: Option[JobnikSession] = parseToJsValue(args(1)).asOpt[JobnikSession]
    val diConf: DIConfiguration = parseToJsValue(args(2)).as[DIConfiguration]
    implicit val jobnikContainer: JobnikContainer = {
      val loggerFactory: LoggerContext = {
        val loggerContext = new LoggerContext()
        val contextInitializer = new ContextInitializer(loggerContext)
        contextInitializer.autoConfig()
        loggerContext
      }
      JobnikContainer(diConf, loggerFactory)
    }

    val jobAborted = for {
      jobnikSession <- maybeJobnikSession
      redisMode <- diConf.redisMode
      jobId <- jobnikSession.jobToken.as[Map[String, String]].get("jobId")
      jobRole = jobnikSession.jobnikRole
      tasksRedis = redisMode.tasks(jobnikSession)
    } yield tasksRedis.exists(s"$jobRole-$jobId-aborted-job")

    Jobnik.monitor("sparkDriver", 2, 2) {
      val configuration = parseConfiguration(args(3))
      val entryPointConfig = createEntryPointConfig(configuration)

      // We have to initialize a SparkSession because yarn.ApplicationMaster expects
      // one to be initialized in every app
      val spark = SparkSession.builder()
        .appName(entryPointConfig.operation)
        .getOrCreate()

      // If we don't have jobnik or redis, assume the job is not aborted
      if (!jobAborted.getOrElse(false)) {
        implicit val endorContext: Context = Context(testMode = testMode)
        spark.withContext(entryPointConfig) {
          run(spark, diConf, configuration.applicationConf)
        }
      }
    }
  }
}
