package org.apache.spark.sql.util

import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.connector.expressions.{FieldReference, Expression => V2Expression}
import org.apache.spark.sql.execution.datasources.v2.V2ColumnUtils
import org.apache.spark.sql.types._

object AggregationUtil {
  def getSchemaForPushedAggregation(aggregation: Aggregation,
                                    schema: StructType): Option[StructType] = {
    def extractColName(v2Expr: V2Expression): Option[String] = v2Expr match {
      case f: FieldReference if f.fieldNames.length == 1 => Some(f.fieldNames.head)
      case _ => None
    }

    var pushAggregationSchema = new StructType()

    def getStructFieldForCol(colName: String): StructField = {
      schema.apply(colName)
    }

    def processMinOrMax(agg: AggregateFunc): Boolean = {
      val (columnName, aggType) = agg match {
        case max: Max if V2ColumnUtils.extractV2Column(max.column).isDefined =>
          (V2ColumnUtils.extractV2Column(max.column).get, "max")
        case min: Min if V2ColumnUtils.extractV2Column(min.column).isDefined =>
          (V2ColumnUtils.extractV2Column(min.column).get, "min")
        case _ => return false
      }


      val structField = getStructFieldForCol(columnName)

      structField.dataType match {
        // not push down complex type
        // not push down Timestamp because INT96 sort order is undefined,
        // Parquet doesn't return statistics for INT96
        // not push down Parquet Binary because min/max could be truncated
        // (https://issues.apache.org/jira/browse/PARQUET-1685), Parquet Binary
        // could be Spark StringType, BinaryType or DecimalType.
        // not push down for ORC with same reason.
        case BooleanType | ByteType | ShortType | IntegerType
             | LongType | FloatType | DoubleType | DateType | StringType | TimestampType =>
          pushAggregationSchema = pushAggregationSchema.add(structField.copy(s"$aggType(" + structField.name + ")"))
          true
        case _ =>
          false
      }
    }

    // First add all not partition col
    aggregation.groupByExpressions.map(extractColName).foreach { colName =>
      pushAggregationSchema = pushAggregationSchema.add(getStructFieldForCol(colName.get))
    }
    aggregation.aggregateExpressions.foreach {
      case max: Max =>
        if (!processMinOrMax(max)) return None
      case min: Min =>
        if (!processMinOrMax(min)) return None
      case count: Count
        if V2ColumnUtils.extractV2Column(count.column).isDefined && !count.isDistinct =>
        val columnName = V2ColumnUtils.extractV2Column(count.column).get
        pushAggregationSchema = pushAggregationSchema.add(StructField(s"cast(count($columnName) as long)", LongType))
      case _: CountStar =>
        pushAggregationSchema = pushAggregationSchema.add(StructField("cast(count(*) as long)", LongType))
      case sum: Sum
        if V2ColumnUtils.extractV2Column(sum.column).isDefined =>
        val columnName = V2ColumnUtils.extractV2Column(sum.column).get
        val col = getStructFieldForCol(columnName)
        col.dataType match {
          case IntegerType =>
            pushAggregationSchema = pushAggregationSchema.add(StructField(s"cast(sum($columnName) as long)", LongType))
          case FloatType =>
            pushAggregationSchema = pushAggregationSchema.add(StructField(s"cast(sum($columnName) as double)", DoubleType))
          case dt =>
            pushAggregationSchema = pushAggregationSchema.add(StructField(s"sum($columnName)", dt))
        }
      case _ => return None
    }

    Some(pushAggregationSchema)
  }
}
