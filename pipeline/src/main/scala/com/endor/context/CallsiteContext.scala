package com.endor.context

import org.apache.spark.SparkContext

import scala.collection.mutable

/**
  * Created by izik on 01/06/2016.
  */
object CallsiteContext {
  def buildJobGroupAndDescription(operation : String,
                                  customer :String,
                                  execId :String,
                                  subIdentifiers : Map[String, String]) : (String, String) = {
    val addedInfo = subIdentifiers.map {
      case (k, v) => s"$k = $v"
    }.mkString(", ")
    val jobGroup = s"$operation (customer = $customer, execution_id = $execId)"
    val description = s"$addedInfo"
    (jobGroup, description)
  }

  val Mock: CallsiteContext = new CallsiteContext()
}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class CallsiteContext {
  private var contextInitParams : Option[(String, String)] = None

  private val contextStack = mutable.Stack[String]()

  private var sparkContext : Option[SparkContext] = None

  def initContext(sc : SparkContext, operation : String,
                  customer :String,
                  execId :String,
                  subIdentifiers : Map[String, String] = Map[String, String]()) : Unit = {
    sparkContext = Option(sc)
    val (builtJobGroup, builtDescription) = CallsiteContext.buildJobGroupAndDescription(operation, customer, execId, subIdentifiers)
    contextInitParams = Option((builtJobGroup, builtDescription))
    sc.setJobGroup(builtJobGroup, builtDescription)
  }

  def enrichContext[T](enrichment : String)(f: => T) : T = {
    def informSparkContextOfCurrentContext(): Unit = {
      for {
        (_, description) <- contextInitParams
        sc <- sparkContext
      } yield sc.setJobDescription(s"$description (${contextStack.reverse.mkString(".")})")
    }

    contextStack.push(enrichment)
    informSparkContextOfCurrentContext()
    val retValue = f
    contextStack.pop()
    informSparkContextOfCurrentContext()
    retValue
  }
}