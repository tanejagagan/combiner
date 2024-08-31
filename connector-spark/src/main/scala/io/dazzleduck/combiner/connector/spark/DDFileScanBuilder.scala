package io.dazzleduck.combiner.connector.spark

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.v2.{FileScanBuilder, TableSampleInfo}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters
import org.apache.spark.sql.jdbc.JdbcDialects
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
  with SupportsPushDownAggregates
  with SupportsPushDownCatalystFilters
  with SupportsPushDownRequiredColumns
  with SupportsPushDownLimit
  with SupportsPushDownTableSample
  with SupportsPushDownTopN {

  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  private val dialect = JdbcDialects.get("jdbc:postgresql")

  private var pushAggregationSchema = new StructType()

  private var pushedAggregations = Option.empty[Aggregation]

  private var tableSample: Option[TableSampleInfo] = None

  private var pushedLimit = 0

  private var sortOrders: Array[String] = Array.empty[String]

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
    new DDScan(sparkSession,  hadoopConf, fileIndex, schema, getReadSchema, _readDataSchema,
      _readPartitionSchema, pushedDataFilters, options, pushedAggregations,
      partitionFilters, dataFilters, tableSample, pushedLimit, sortOrders )
  }

  override def readDataSchema(): StructType = {
    if(pushedAggregations.nonEmpty) {
      StructType(pushAggregationSchema.fields.filter( n => !partitionNameSet.contains(n.name)))
    } else {
      super.readDataSchema()
    }
  }

  private def getReadSchema: StructType = {
    if(pushAggregationSchema.nonEmpty) {
      pushAggregationSchema
    } else {
      new StructType(readDataSchema().fields ++ readPartitionSchema().fields)
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
    val untranslated = mutable.ArrayBuffer.empty[Expression]
    for (filterExpr <- dataFilters) {
      val translated = SparkInternalAccessUtil.translateFilter(filterExpr)
      translated match {
        case Some(x) => translatedFilters += x
        case None => untranslated += filterExpr
      }
    }
    this.pushedDataFilters = translatedFilters.toArray
    nonDeterminsticFilters ++ untranslated
  }

  override def pushTableSample( lowerBound: Double,
                                upperBound: Double,
                                withReplacement: Boolean,
                                seed: Long): Boolean = {
    this.tableSample = Some(TableSampleInfo(lowerBound, upperBound, withReplacement, seed))
    return true
  }

  override def pushLimit(limit: Int): Boolean = {
      pushedLimit = limit
      return true
  }

  override def pushTopN(orders: Array[SortOrder], limit: Int): Boolean = {
    val compiledOrders = orders.flatMap(dialect.compileExpression(_))
      if (orders.length != compiledOrders.length) return false
      pushedLimit = limit
      sortOrders = compiledOrders
      return true
  }

  override def isPartiallyPushed: Boolean = true

  override def pruneColumns(requiredSchema: StructType): Unit = {
    super.pruneColumns(requiredSchema)
  }

  override protected val supportsNestedSchemaPruning = true
}
