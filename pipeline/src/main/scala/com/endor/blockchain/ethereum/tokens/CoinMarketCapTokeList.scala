package com.endor.blockchain.ethereum.tokens

import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.scraper.ContentExtractors._

object CoinMarketCapTokeList {
  def get(): Seq[String] = {
    val browser = JsoupBrowser.typed()
    val doc = browser.get("https://coinmarketcap.com/tokens/views/all/")
    val names = (doc >> elements("td[class='no-wrap currency-name']"))
      .map(_ >> element("a[class='currency-name-container']"))
      .map(_.text.trim)
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
