package io.dazzleduck.combiner.connector.spark

import io.dazzleduck.combiner.common.model.QueryObject
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import java.util
import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}

/**
 * A factory used to create Parquet readers.
 *
 * @param sqlConf
 * @param broadcastedConf
 * @param dataSchema  Schema of Parquet files.
 * @param readDataSchema  Required schema of Parquet files.
 * @param partitionSchema Schema of partitions.
 * @param filters
 * @param aggregation
 * @param options
 */

case class DDFilePartitionReaderFactory(sqlConf: SQLConf,
                                        broadcastedConf: Broadcast[SerializableConfiguration],
                                        partitionSchema: StructType,
                                        filters: Array[Filter],
                                        outputSchema: StructType,
                                        aggregation: Option[Aggregation],
                                        options: ParquetOptions,
                                        parameters : Map[String, String])
  extends FilePartitionReaderFactory {

  val fileSystemParameterPrefix = "fs.s3a."
  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new IllegalArgumentException("row based reader not supported")
  }

  override def buildColumnarReader(partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val connectionUrl = DDReader.connectionUrl(parameters)
    if(connectionUrl != null){
      assert(partitionedFile.filePath.toUri.getScheme != "file",
        s"Do support File with connection Url $connectionUrl")
    }
    val file = uriToPath(partitionedFile.filePath.toUri)
    val groupBys = aggregation.toList.flatMap(a => a.groupByExpressions())
    val sources = new util.ArrayList[String]()
    sources.add(file)

    val partitionSchemaFields = partitionSchema.fields.map(_.name).toSet
    val requiredPartitionSchema = outputSchema.fields.filter( f => partitionSchemaFields.contains(f.name))
    val requiredDataSchema =  outputSchema.fields.filterNot( f => partitionSchemaFields.contains(f.name))
    val queryObjectBuilder = QueryObject.builder()
      .predicates(filters.map(_.toString).toList.asJava)
      .projections(requiredDataSchema.map(_.name).toList.asJava)
      .sources(sources)

    if (groupBys.nonEmpty) {
      queryObjectBuilder.groupBys(groupBys.map(_.toString).asJava)
    }

    if(partitionedFile.filePath.toUri.getScheme() == "s3a") {
      val m = new util.HashMap[String, String]()
      val queryParameters = DDReader.getDuckDBParameters(fileSystemParameterPrefix, broadcastedConf.value.value)
      m.putAll(queryParameters)
      m.putAll(parameters.asJava)
      queryObjectBuilder.parameters(m)
    }
    DDReader(queryObjectBuilder.build(), parameters, new StructType(requiredPartitionSchema), partitionedFile.partitionValues)
  }

  def uriToPath(uri: URI): String = {
    if (uri.getScheme == "file") {
      uri.getPath
    } else {
      uri.toString
    }
  }

  def constructHiveTypeString(partitionSchema: StructType): String = {
    partitionSchema.map(f => s"'${f.name}' : ${f.dataType.sql}").mkString("{", ",", "}")
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    true
  }
}



