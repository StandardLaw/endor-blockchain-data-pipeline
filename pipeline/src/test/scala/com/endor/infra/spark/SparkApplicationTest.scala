package com.endor.infra.spark

import com.amazonaws.auth.{AWSCredentialsProviderChain, InstanceProfileCredentialsProvider}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import org.apache.spark.sql.test.SharedSQLContext
import play.api.libs.json._

class SparkApplicationTest extends SharedSQLContext {
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def debugSparkApplication(json: JsObject): Unit = {
    val credentials = new AWSCredentialsProviderChain(
      new ProfileCredentialsProvider(),
      InstanceProfileCredentialsProvider.getInstance()
    ).getCredentials
    sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", credentials.getAWSAccessKeyId)
    sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", credentials.getAWSSecretKey)
    val diConfigurationJson = (json \ "diConfiguration").as[JsObject].deepMerge(Json.parse(
      """
        |{
        | "sparkInfrastructure": {
        |   "typeName": "Local"
        | }
        |}
      """.stripMargin).as[JsObject])
    val className = (json \ "classFqdn").as[String]
    val application = Class.forName(className, true, this.getClass.getClassLoader)
    val mainMethod = application.getMethod("main", new Array[String](0).getClass)
    mainMethod.invoke(null, Array("debugDriver",
      (json \ "jobnikSession").asOpt[JsObject].map(_.toString).getOrElse("null"),
      diConfigurationJson.toString,
      (json \ "arg").as[JsObject].toString
    ))
  }

  ignore("debug") {
    val json = Json.parse(
      """{
        |    "arg": {
        |        "applicationConf": {
        |            "output": "s3://source-ethereum/TransactionsNew/Inbox/",
        |            "input": "s3://endor-blockchains/ethereum/blocks/InProgress//"
        |        },
        |        "featureFlags": {
        |            "debugQueryBuilder": false,
        |            "partitioningMethod": {
        |                "typeName": "RandomBased"
        |            },
        |            "multiChannelPubSub": true
        |        }
        |    },
        |    "jarPath": "",
        |    "classFqdn": "com.endor.blockchain.ethereum.transaction.EMRTransactionsPipeline",
        |    "jobnikSession": null,
        |    "additionalSparkConf": {
        |        "spark.files.maxPartitionBytes": "1"
        |    },
        |    "diConfiguration": {
        |        "artifactPublishers": {
        |            "typeName": "Real"
        |        },
        |        "engineCommunicationType": {
        |            "typeName": "S3"
        |        },
        |        "userProfiler": {
        |            "typeName": "DatasetUserProfiler"
        |        },
        |        "dataFrameSource": {
        |            "typeName": "S3"
        |        },
        |        "sampledDataSource": {
        |            "typeName": "S3"
        |        },
        |        "jsonSource": {
        |            "typeName": "S3"
        |        },
        |        "publicationMedium": {
        |            "typeName": "AmazonSQS"
        |        },
        |        "sparkInfrastructure": {
        |            "isPrimary": true,
        |            "typeName": "EMR"
        |        },
        |        "redisMode": {
        |            "typeName": "Real"
        |        }
        |    }
        |}""".stripMargin)
    debugSparkApplication(json.as[JsObject])
  }
}
