package com.endor.storage.io

trait IOHandlerComponent {
  implicit def ioHandler: IOHandler
}
