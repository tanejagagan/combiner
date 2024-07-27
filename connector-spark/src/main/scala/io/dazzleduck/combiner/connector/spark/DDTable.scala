package io.dazzleduck.combiner.connector.spark

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class DDTable(override val name: String,
              val sparkSession: SparkSession,
              val options: CaseInsensitiveStringMap,
              val paths: Seq[String],
              val userSpecifiedSchema: Option[StructType],
              override val fallbackFileFormat: Class[_ <: FileFormat]) extends FileTable(
  sparkSession, options, paths, userSpecifiedSchema) {

  override def newScanBuilder(options: CaseInsensitiveStringMap): DDFileScanBuilder = {
    val newOptions = new util.HashMap[String, String](this.options);
    newOptions.putAll(options);
    new DDFileScanBuilder(sparkSession, fileIndex, schema, dataSchema, new CaseInsensitiveStringMap(newOptions))
  }

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = None

  override def formatName: String = "dd"

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    throw new UnsupportedOperationException("Write is not supported")
  }
}