package io.dazzleduck.combiner.connector.spark

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.{AggregationUtil, CaseInsensitiveStringMap, SparkInternalAccessUtil}
import org.apache.spark.sql.{SparkSession, sources}

import scala.collection.JavaConverters._
import scala.collection.mutable

class DDFileScanBuilder(sparkSession: SparkSession,
                        fileIndex: PartitioningAwareFileIndex,
                        val schema: StructType,
                        dataSchema: StructType,
                        options: CaseInsensitiveStringMap) extends  FileScanBuilder(sparkSession, fileIndex, dataSchema)
  with SupportsPushDownAggregates {

  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  private var pushAggregationSchema = new StructType()

  private var pushedAggregations = Option.empty[Aggregation]

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    AggregationUtil.getSchemaForPushedAggregation(
      aggregation,
      schema) match {

      case Some(schema) =>
        pushAggregationSchema = schema
        this.pushedAggregations = Some(aggregation)
        true
      case _ => false
    }
  }

  override def build(): DDScan = {
    // the `finalSchema` is either pruned in pushAggregation (if aggregates are
    // pushed down), or pruned in readDataSchema() (in regular column pruning). These
    // two are mutual exclusive.

    val _readDataSchema = readDataSchema()
    val _readPartitionSchema = readPartitionSchema()
    new DDScan(sparkSession, hadoopConf, fileIndex, schema, _readDataSchema,
      _readPartitionSchema, pushedDataFilters, options, pushedAggregations,
      partitionFilters, dataFilters)
  }

  override def readDataSchema(): StructType = {
    if(pushedAggregations.nonEmpty) {
      StructType(pushAggregationSchema.fields.filter( n => !partitionNameSet.contains(n.name)))
    } else {
      super.readDataSchema()
    }
  }

  override def readPartitionSchema(): StructType = {
    if(pushedAggregations.nonEmpty) {
      val fields =  pushAggregationSchema.fields.filter( n => partitionNameSet.contains(n.name))
      new StructType(fields)
    } else {
      super.readPartitionSchema()
    }
  }

  override def pushFilters(filters: Seq[Expression]): Seq[Expression] = {
    val (deterministicFilters, nonDeterminsticFilters) = filters.partition(_.deterministic)
    val (partitionFilters, dataFilters) =
      DataSourceUtils.getPartitionFiltersAndDataFilters(fileIndex.partitionSchema, deterministicFilters)
    this.partitionFilters = partitionFilters
    this.dataFilters = dataFilters
    val translatedFilters = mutable.ArrayBuffer.empty[sources.Filter]
    for (filterExpr <- dataFilters) {
      val translated = SparkInternalAccessUtil.translateFilter(filterExpr)
      if (translated.nonEmpty) {
        translatedFilters += translated.get
      }
    }
    pushedDataFilters = pushDataFilters(translatedFilters.toArray)
    nonDeterminsticFilters
  }
}
