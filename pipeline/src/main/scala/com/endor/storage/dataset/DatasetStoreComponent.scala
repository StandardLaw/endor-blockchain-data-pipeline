package com.endor.storage.dataset

import com.endor.infra.{DIConfigurationComponent, SparkSessionComponent}
import com.endor.storage.io.{IOHandlerComponent, IoHandlerType}

trait DatasetStoreComponent {
  this: DIConfigurationComponent with SparkSessionComponent with IOHandlerComponent =>

  implicit lazy val datasetStore: DatasetStore = {
    diConfiguration.dataFrameSource match {
      case IoHandlerType.InMemory => new InMemoryDatasetStore()
      case ioHandlerType => new FileSystemDatasetStore(ioHandler(ioHandlerType), spark)
    }
  }
}