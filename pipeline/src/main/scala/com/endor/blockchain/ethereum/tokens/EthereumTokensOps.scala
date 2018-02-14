package com.endor.blockchain.ethereum.tokens

import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.scraper.ContentExtractors._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object EthereumTokensOps {
  final case class TokenData(name: String, address: Option[String])

  def trimName(name: String): String = name match {
    case x if x.length > 16 => x.substring(0, 13).trim.stripSuffix("-")
    case x => x.trim
  }
  val trimNameUdf: UserDefinedFunction = udf(trimName _)

  def normalizeName(name: String): String = name.toLowerCase.replace("...", "").trim.replace(" ", "-")
  val normalizeNameUdf: UserDefinedFunction = udf(normalizeName _)

  trait TokenListScraper {
    def scrape(): Seq[String]
  }

  @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
  val coinMarketCapScraper: TokenListScraper = () => {
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

  private def getAddressFromEtherscanURL(url: String): Option[String] = {
    val browser = JsoupBrowser.typed()
    val doc = browser.get(url)
    doc >?> element("#ContentPlaceHolder1_trContract") >> element("a") >> text
  }

  private val ethplorerAddressRegex = "https://ethplorer.io/address/0x(.*)".r

  private def getAddressFromEthplorerURL(url: String): Option[String]= {
    url match {
      case ethplorerAddressRegex(address) => Option(address)
      case _ => None
    }
  }

  def scrapeTokenData(): Seq[TokenData] = {
    val browser = JsoupBrowser.typed()
    val doc = browser.get("https://coinmarketcap.com/tokens/views/all/")
    val namesAndURLs = (doc >> elements("td[class='no-wrap currency-name']"))
      .map(_ >> element("a[class='currency-name-container']"))
      .map(aTag => (normalizeName(aTag.text), "https://coinmarketcap.com" + aTag.attr("href")))
      .toSeq
    val platforms = (doc >> elements("td[class='no-wrap platform-name']"))
      .map(_ >> element("a"))
      .map(_.text.toLowerCase)
      .toSeq
    namesAndURLs
      .zip(platforms)
      .filter(_._2 == "ethereum")
      .map(_._1)
      .flatMap {
        case (name, url) =>
          val tokenDoc = browser.get(url)
          (tokenDoc >?> elementList("li") >/~ validator(elementList("span[class='glyphicon glyphicon-search text-gray']"))(_.nonEmpty) >> element("a") >> attr("href"))
            .map(_.collect {
              case Right(newUrl) => newUrl
            })
            .map(results => {
              val ethplorerResult = results.map(getAddressFromEthplorerURL).reduceOption(_ orElse _).flatten
              results.foldLeft(ethplorerResult)((r, url) => r orElse getAddressFromEtherscanURL(url))
            })
            .map(TokenData(name, _))

      }
  }
}
