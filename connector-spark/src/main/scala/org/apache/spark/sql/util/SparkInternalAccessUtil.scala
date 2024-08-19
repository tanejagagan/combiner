package org.apache.spark.sql.util

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.Filter

object SparkInternalAccessUtil {
  def translateFilter( predicate: Expression ) =  DataSourceStrategy.translateFilter(predicate, true)
  def toV2( filter : Filter ) : Predicate  = filter.toV2
}
