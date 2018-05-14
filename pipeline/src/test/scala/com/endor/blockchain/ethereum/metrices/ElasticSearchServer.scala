package com.endor.blockchain.ethereum.metrices

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.Node

class ElasticSearchServer (){

  private lazy val node = new Node(ElasticSearchServer.settings)
 // private lazy val node = LocalNodeFactory.nodeBuilder().local(true).settings(settings).build
  def client: Client = node.client()

  def start(): Unit = {
    node.start()
  }

  def stop(): Unit = {
    node.close()

    try {
      FileUtils.forceDelete(ElasticSearchServer.dataDir)
    } catch {
      case e: Exception => // dataDir cleanup failed
    }
  }
  def createAndWaitForIndex(index: String): Unit = {
    client.admin.indices.prepareCreate(index).execute.actionGet()
    client.admin.cluster.prepareHealth(index).setWaitForActiveShards(1).execute.actionGet()
  }
}

object ElasticSearchServer {
  private val clusterName = "elasticsearch"
  private val dataDir = Files.createTempDirectory("elasticsearch_data_").toFile
  private val settings = Settings.builder()
    .put("path.data", ElasticSearchServer.dataDir.toString)
    .put("cluster.name", ElasticSearchServer.clusterName)
    .build
}