package com.endor.infra

trait Driver[C] {
  def run(configuration: C): Unit
}

trait DriverComponent[T <: Driver[_]] {
  def driver: T
}
