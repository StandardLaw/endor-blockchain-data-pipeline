package com.endor.storage

import org.apache.spark.sql.Row

package object sources extends CustomerSources with DataSources {
  type DataFrameSource = DatasetSource[Row]
}
