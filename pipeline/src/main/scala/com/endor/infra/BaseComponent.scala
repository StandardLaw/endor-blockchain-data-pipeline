package com.endor.infra

import com.endor.storage.dataset.DatasetStoreComponent
import com.endor.storage.io.IOHandlerComponent

trait BaseComponent extends DIConfigurationComponent with SparkSessionComponent
  with IOHandlerComponent with DatasetStoreComponent
