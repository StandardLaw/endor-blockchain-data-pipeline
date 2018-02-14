package com.endor.storage.sources

import com.endor.CustomerId


trait CustomerSources {
  implicit final class CustomerFileSource(customer: CustomerId) {
    def source: FileSource = new FileSource(s"source-${customer.id}")
    def sandbox: FileSource = new FileSource(s"sandbox-${customer.id}")
  }
}