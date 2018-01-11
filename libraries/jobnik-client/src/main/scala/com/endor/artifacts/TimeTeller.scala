package com.endor.artifacts

import java.util.{Calendar, TimeZone}

import org.joda.time.DateTime

/**
  * Created by izik on 07/06/2016.
  */
trait TimeTeller {
  private val calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
  //noinspection AccessorLikeMethodIsEmptyParen
  def getUTCNow(): DateTime = {
    new DateTime(calendar.getTime)
  }
}
