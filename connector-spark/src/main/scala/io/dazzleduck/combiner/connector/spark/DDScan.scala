package io.dazzleduck.combiner.connector.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters.mapAsScalaMapConverter

class DDScan(override val sparkSession: SparkSession,
             val hadoopConf: Configuration,
             override val fileIndex: PartitioningAwareFileIndex,
             override val dataSchema: StructType,
             override val readDataSchema: StructType,
             override val readPartitionSchema: StructType,
             val pushedFilters: Array[Filter],
             val options: CaseInsensitiveStringMap,
             val pushedAggregate: Option[Aggregation],
             override val partitionFilters: Seq[Expression],
             override val dataFilters: Seq[Expression]) extends FileScan {


  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    val sqlConf = sparkSession.sessionState.conf

    new DDFilePartitionReaderFactory(
      sqlConf,
      broadcastedConf,
      fileIndex.partitionSchema,
      pushedFilters,
      readSchema(),
      pushedAggregate,
      new ParquetOptions(options.asCaseSensitiveMap.asScala.toMap, sqlConf),
      options.asCaseSensitiveMap().asScala.toMap)
  }
}
