package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.DataSourceStrategy

object SparkInternalAccessUtil {
  def translateFilter( predicate: Expression ) =  DataSourceStrategy.translateFilter(predicate, true)
}
