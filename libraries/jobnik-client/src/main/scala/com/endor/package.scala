package com

/**
  * Created by user on 14/05/17.
  */
package object endor {
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  implicit final class AnyOps[A](self: A) {
    def ===(other: A): Boolean = self == other
    def !==(other: A): Boolean = self != other
  }
}
