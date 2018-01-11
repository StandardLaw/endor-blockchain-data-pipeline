package com.endor.context

final case class Context(callsiteContext : CallsiteContext = new CallsiteContext(),
                         testMode: Boolean = false)
