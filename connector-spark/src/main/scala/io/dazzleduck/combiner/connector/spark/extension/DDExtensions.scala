package io.dazzleduck.combiner.connector.spark.extension

import io.dazzleduck.combiner.connector.spark.DDScan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, SparkSessionExtensionsProvider}

case class RemoveHashAggregate(sparkSession: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = removeHashAggregate(plan)

  def removeHashAggregate(plan: SparkPlan): SparkPlan = {
    plan match {
      case exec @ HashAggregateExec(_, _, _, _, _ , _, _, _, child @ ProjectExec(a, b@BatchScanExec(_,scan : DDScan, _, _, _,_)))
        if(scan.pushedAggregate.nonEmpty) => child
      case x => x
    }
  }
}

class DDExtensions extends SparkSessionExtensionsProvider {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectQueryStageOptimizerRule(session => new RemoveHashAggregate(session))
  }
}