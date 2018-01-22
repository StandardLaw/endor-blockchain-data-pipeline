package com.endor.infra

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.util.ContextInitializer

object LoggingComponent {
  lazy val inMemoryLoggerFactory: LoggerContext = {
    val loggerContext = new LoggerContext()
    val contextInitializer = new ContextInitializer(loggerContext)
    val configurationUri = this.getClass.getClassLoader.getResource("logback-inmem.xml")
    contextInitializer.configureByResource(configurationUri)
    loggerContext
  }
}

trait LoggingComponent {
  implicit lazy val loggerFactory: LoggerContext = {
    val loggerContext = new LoggerContext()
    val contextInitializer = new ContextInitializer(loggerContext)
    contextInitializer.autoConfig()
    loggerContext
  }
}
