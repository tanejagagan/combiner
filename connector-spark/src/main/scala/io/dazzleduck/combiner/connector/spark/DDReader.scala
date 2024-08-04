package io.dazzleduck.combiner.connector.spark

import io.dazzleduck.combiner.common.{ConfigParameters, ParameterHelper, Pools}
import io.dazzleduck.combiner.common.client.CombinerClient
import io.dazzleduck.combiner.common.model.{QueryGenerator, QueryObject}
import io.dazzleduck.combiner.common.model.QueryObject
import io.dazzleduck.combiner.common.Pools
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowReader, ArrowStreamReader}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.duckdb.DuckDBResultSet

import java.net.URI
import java.util.{Map => JMap}
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter}

object DDReader {
  val hadoopToDuckDBParamValueFu = Map[String, (String, String=> String)](
    "secret.key" -> ("s3_secret_access_key", v => v),
    "access.key" -> ("s3_access_key_id", v => v),
    "endpoint" -> ("s3_endpoint" , v => {
      val uri = URI.create(v)
      uri.getHost + ":" + uri.getPort()
    }),
    "connection.ssl.enabled" ->("s3_use_ssl",  v=> v)
  )

  def apply( outputSchema : StructType,
             queryObject: QueryObject,
            parameters  : Map[String, String],
            requiredPartitionSchema : StructType,
            requiredPartitions : InternalRow) : DDReader = {
    val connectionUrl = DDReader.connectionUrl(parameters)
    if (connectionUrl == null) {
      new DDDirectReader(outputSchema, queryObject, parameters.asJava, requiredPartitionSchema, requiredPartitions)
    } else {
      new DDWebReader(outputSchema, queryObject, parameters.asJava, requiredPartitionSchema, requiredPartitions)
    }
  }

  def connectionUrl(parameter : Map[String, String]): String = {
    ParameterHelper.getConnectionUrl(parameter.asJava)
  }

  def getDuckDBParameters(prefix : String, hadoopConf : Configuration) : JMap[String, String] = {
    val result = new java.util.HashMap[String, String] ()
    hadoopToDuckDBParamValueFu.foreach{ case(k, (ddKey, valueFn)) =>
      val key = prefix + k
      val value = hadoopConf.get(key)
      if(value !=null){
        result.put(ddKey, valueFn(value))
      }
    }
    result
  }
}
abstract class DDReader( outputSchema : StructType,
                         requiredPartitionSchema : StructType,
                        requiredPartitions : InternalRow) extends PartitionReader[ColumnarBatch]{

  var hasNext : Boolean = _

  override def next(): Boolean = {
    getHasNext
  }

  def getHasNext : Boolean = this.hasNext

  override def get(): ColumnarBatch = {
    val reader = getReader()
    val vsr = reader.getVectorSchemaRoot
    val valueVectors = getValueVector(vsr)
    val partitionVectors = createPartitionVectors(vsr.getRowCount)
    val res: ColumnarBatch = new ArrowColumnarBatch(partitionVectors ++ valueVectors,
      vsr.getRowCount, null)
    setHasNext(reader.loadNextBatch())
    res
  }

  def getValueVector(vsr: VectorSchemaRoot): Array[ColumnVector] = {
    vsr.getFieldVectors.asScala.map(fv => new ArrowColumnVector(vsr.getVector(fv.getName))).toArray
  }

  def createPartitionVectors(size : Int ) : Array[ColumnVector] = {
    requiredPartitionSchema.fields.zipWithIndex.map { case (f, index) =>
      val vector = new ConstantColumnVector(size, f.dataType)
      f.dataType match {
        case IntegerType => vector.setInt( requiredPartitions.getInt(index))
        case LongType => vector.setLong(requiredPartitions.getLong(index))
        case StringType => vector.setUtf8String(requiredPartitions.getUTF8String(index))
        case DateType => vector.setInt( requiredPartitions.getInt(index))
        case d : DecimalType => vector.setDecimal(
          requiredPartitions.getDecimal(index, d.precision, d.scale), d.precision)
        case TimestampType => vector.setLong(requiredPartitions.getLong(index))
        case TimestampNTZType => vector.setLong(requiredPartitions.getLong(index))
      }
      vector
    }
  }

  def setHasNext(n : Boolean): Unit = {
    this.hasNext = n
  }

  override def close(): Unit = {
    getReader().close()
  }

  def getReader() : ArrowReader
}

class DDWebReader(outputSchema : StructType, queryObject: QueryObject,
                  parameters  : JMap[String, String],
                  requiredPartitionSchema : StructType,
                  requiredPartitions : InternalRow,
                 ) extends DDReader(outputSchema, requiredPartitionSchema, requiredPartitions) {

  private val connectionUrl = ParameterHelper.getConnectionUrl(parameters)
  private val stream = CombinerClient.getArrowStream(connectionUrl,
    queryObject)
  private val reader = new ArrowStreamReader(stream, Pools.ALLOCATOR_POOL.get())
  setHasNext(reader.loadNextBatch())

  override def getReader(): ArrowReader = reader

  override def close(): Unit = {
    super.close()
    stream.close()
  }
}

class DDDirectReader(outputSchema : StructType,
                     queryObject: QueryObject,
                     parameters  : JMap[String, String],
                     requiredPartitionSchema : StructType,
                     requiredPartitions : InternalRow,
                    ) extends DDReader(outputSchema, requiredPartitionSchema, requiredPartitions) {

  private val batchSize = ConfigParameters.getArrowBatchSize(queryObject.getParameters)
  private val statement = QueryGenerator.DUCK_DB.generate(queryObject, "parquet")
  private val conn = Pools.DD_CONNECTION_POOL.get()
  private val p_stmt = conn.prepareStatement(statement)
  private val resultSet = p_stmt.executeQuery.asInstanceOf[DuckDBResultSet]
  private val allocator = Pools.ALLOCATOR_POOL.get()
  private val reader = resultSet.arrowExportStream(allocator, batchSize).asInstanceOf[ArrowReader]
  super.setHasNext(reader.loadNextBatch())

  override def getReader(): ArrowReader = reader

  override def close(): Unit = {
    super.close()
    resultSet.close()
  }
}



