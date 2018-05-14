package com.endor.blockchain.ethereum.metrices

import com.endor.infra.spark.SparkDriverFunSuite
//import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest
//import org.elasticsearch.spark.sql._

class EthereumTokenMatricesTest extends SparkDriverFunSuite {


    test("basic-matrices-test") {
      val hello = new ElasticSearchServer()
      print("no cluster")
      Thread.sleep(60000)
      hello.start()
      hello.createAndWaitForIndex("hello")
      Thread.sleep(60000)
      hello.client.admin().cluster().clusterStats(new ClusterStatsRequest("*"))
      //val df = spark.read.json("/Users/yuval/people.json")

//      val sess = spark
//      import sess.implicits._
//      val data = (0 to 10).map(_ => generateRandomRow("myToken", "MT", "0x1337"))
//      val ds = spark.createDataset(data)
//      val expectedOpenRate = data.minBy(_.timestamp.getTime).price
//      val openRate = ds.agg(open($"timestamp", $"price")).as[Double].head()
//      openRate should equal(expectedOpenRate)
    }

}
