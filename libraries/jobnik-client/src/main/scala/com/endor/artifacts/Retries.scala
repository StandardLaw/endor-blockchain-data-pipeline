package com.endor.artifacts

import scala.concurrent.duration._


/**
  * Created by izik on 28/09/2016.
  */
object Retries {
  private val Nop = () => {}
  private val NopInt = (_ : Int) => {}

  @annotation.tailrec
  def retry[T](n: Int, onAttemptFailure : Int => Unit = NopInt,
               onFailure : () => Unit = Nop)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 =>
        onAttemptFailure(n)
        retry(n - 1)(fn)
      case util.Failure(e) =>
        onFailure()
        throw e
    }
  }

  @annotation.tailrec
  def retryWithExponentialBackoff[T](n: Int, backoff : FiniteDuration = 1 seconds,
                                     multiplicationFactor : Int = 2,
                                     onAttemptFailure : Int => Unit = NopInt,
                                     onFailure : () => Unit = Nop)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 =>
        onAttemptFailure(n)
        Thread.sleep(backoff.toMillis)
        retryWithExponentialBackoff(n - 1, backoff * 2, multiplicationFactor, onAttemptFailure, onFailure)(fn)
      case util.Failure(e) =>
        onFailure()
        throw e
    }
  }

}
