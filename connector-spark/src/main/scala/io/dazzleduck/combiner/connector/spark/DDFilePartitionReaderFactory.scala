package io.dazzleduck.combiner.connector.spark

import io.dazzleduck.combiner.common.model.QueryObject
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, V2ExpressionUtils}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, TableSampleInfo}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, MapType, StructField, StructType}
import org.apache.spark.sql.util.SparkInternalAccessUtil
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
                                        outputSchema : StructType,
                                        readDataSchema: StructType,
                                        readPartitionSchema : StructType,
                                        aggregation: Option[Aggregation],
                                        options: ParquetOptions,
                                        parameters : Map[String, String],
                                        tableSample: Option[TableSampleInfo],
                                        pushedLimit: Int,
                                        sortOrders: Array[String])
  extends FilePartitionReaderFactory {

  // Check for map type
  if(containsMapType(outputSchema)){
    throw new RuntimeException(s"Output schema contains MapType ${outputSchema} which is not yet supported")
  }

  private val fileSystemParameterPrefix = "fs.s3a."
  private val dialect = JdbcDialects.get("jdbc:postgresql")

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

    val _filters = filters.flatMap( f =>
      dialect.compileExpression(SparkInternalAccessUtil.toV2(f)).toSeq).toList.asJava
    val queryObjectBuilder = QueryObject.builder()
      .predicates(_filters)
      .projections(readDataSchema.map( f => constructProject(f, None)).toList.asJava)
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
    DDReader(outputSchema, queryObjectBuilder.build(), parameters, readDataSchema, readPartitionSchema, partitionedFile.partitionValues)
  }

  private def constructProject(field  : StructField, parentRef : Option[String]) : String = {
    (field.dataType, parentRef) match {
      case (StructType(fields), _ ) =>
        val ref = parentRef match {
          case Some(p) => p + "." +  field.name
          case None => field.name
        }
        val expr = fields.map(f => f.name -> constructProject(f, Some(ref)))
          .map{ case (name, e) => name  + ":=" + e}
        s"struct_pack( ${expr.mkString(",")})"
      case (_, Some(p)) => p + "." + field.name
      case (_, None) => field.name
    }
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

  private def containsMapType(dataType: DataType ) : Boolean = {
    dataType match {
      case s : StructType =>
        s.fields.exists(f => containsMapType(f.dataType))
      case map : MapType => true
      case _ => false
    }
  }
}



