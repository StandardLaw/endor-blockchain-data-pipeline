package com.endor.blockchain.ethereum.tokens

import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.scraper.ContentExtractors._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object EthereumTokensOps {
  def trimName(name: String): String = name match {
    case x if x.length > 16 => x.substring(0, 13).trim.stripSuffix("-")
    case x => x.trim
  }
  val trimNameUdf: UserDefinedFunction = udf(trimName _)

  def normalizeName(name: String): String = name.toLowerCase.replace("...", "").trim.replace(" ", "-")
  val normalizeNameUdf: UserDefinedFunction = udf(normalizeName _)

  def scrapeTokenList(): Seq[String] = {
    val browser = JsoupBrowser.typed()
    val doc = browser.get("https://coinmarketcap.com/tokens/views/all/")
    val names = (doc >> elements("td[class='no-wrap currency-name']"))
      .map(_ >> element("a[class='currency-name-container']"))
      .map(_.text)
      .map(normalizeName)
      .toSeq
    val platforms = (doc >> elements("td[class='no-wrap platform-name']"))
      .map(_ >> element("a"))
      .map(_.text.toLowerCase)
      .toSeq
    names
      .zip(platforms)
      .filter(_._2 == "ethereum")
      .map(_._1)
  }
}
