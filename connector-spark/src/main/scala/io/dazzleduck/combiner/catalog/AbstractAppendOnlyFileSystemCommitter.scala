package io.dazzleduck.combiner.catalog

import com.fasterxml.jackson.databind.ObjectMapper
import io.dazzleduck.combiner.catalog.AppendOnlyFileSystemCommitter.Result
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileStatus, Path}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.util
import java.util.function.Function
import java.util.{Collections, Optional, function}
import scala.collection.JavaConverters.{asJavaIterableConverter, mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.compat.Platform.ConcurrentModificationException
import scala.io.{Source => IOSource}

abstract class AbstractAppendOnlyFileSystemCommitter[M, R, K] extends AppendOnlyFileSystemCommitter[M, R, K] with DAO[R, K] {

  val refreshInterval = 5 * 60 * 1000 // 5 min
  var latestRefreshTime = 0l
  var _latestState: Option[State[M, R, K]] = None

  @throws[ConcurrentModificationException]
  def append(data: util.Collection[R], metadata: M): Boolean = synchronized {
    val time = System.currentTimeMillis()
    val oBatchId = getLatestBatchIdFromListing
    val nextBatchId: Long = oBatchId.map(_ + 1).getOrElse(0L)
    val fileManager = getFileManager
    val path = getPath
    val (oHeader, newMd) = oBatchId match {
      case Some(batchId) =>
        val (header, oldMetadata) = readHeaderAndMetadata(batchId)
        val pr = proceed(nextBatchId, oldMetadata, metadata)
        if (pr == Result.NO_OP) {
          return false
        }
        if (pr == Result.CONCURRENT_MODIFICATION) {
          throw new ConcurrentModificationException("resource modified concurrently")
        }
        (Some(header), resolveMetaData(nextBatchId, oldMetadata, metadata))
      case None =>
        (None, resolveMetaData(nextBatchId, null.asInstanceOf[M], metadata));
    }

    val validBatches = oHeader match {
      case Some(header) => header.validBatches
      case None => Array[Long]()
    }

    val filePath = new Path(path, nextBatchId.toString)
    val nc = needCompaction(fileManager.list(path).filter(f => validBatches.contains(f.getPath.getName.toLong)))
    if (nc) {
      val compactHeader = new Header(Array(nextBatchId), nextBatchId, time)
      write(filePath, stream => {
        completeWriteWithCompression(nextBatchId, data, stream, compactHeader, newMd, validBatches)
      })
    } else {
      val header = new Header(validBatches :+ (nextBatchId), nextBatchId, time)
      write(filePath, stream => {
        writeHeadAndMd(stream, header, newMd)
        appendWrite(nextBatchId, stream, data)
      })
    }
    true

  }

  def getLatestBatchIdFromListing: Option[Long] = {
    val path = getPath
    val fileManager = getFileManager
    latestRefreshTime = System.currentTimeMillis()
    fileManager.list(path).map(f => f.getPath.getName.toLong).sorted.reverse.headOption
  }

  private def needCompaction(validBatches: Array[FileStatus]): Boolean = {
    validBatches.map(_.getLen).size > maxUnCompactedFiles
  }

  private def readHeaderAndMetadata(batchId: Long): (Header, M) = {
    val fileManager = getFileManager
    val path = getPath
    val fsStream = fileManager.open(new Path(path, batchId.toString))
    val lines = IOSource.fromInputStream(fsStream, UTF_8.name()).getLines()
    val firstLine = lines.next()
    val secondLine = lines.next();
    val header = headerDeSerializer.apply(firstLine)
    val metadata = metadataDeSerializer.apply(secondLine)
    (header, metadata)
  }

  protected def headerDeSerializer: function.Function[String, Header] = str => {
    new ObjectMapper().readValue(str, classOf[Header])
  }

  protected def write(batchMetadataFile: Path,
                      fn: OutputStream => Unit): Unit = {
    val fileManager = getFileManager
    // Only write metadata when the batch has not yet been written
    val output = fileManager.createAtomic(batchMetadataFile, overwriteIfPossible = false)
    try {
      fn(output)
      output.close()
    } catch {
      case e: FileAlreadyExistsException =>
        output.cancel()
        // If next batch file already exists, then another concurrently running query has
        // written it.
        throw new ConcurrentModificationException(batchMetadataFile.toString + "already exists")
      case e: Throwable =>
        output.cancel()
        throw e
    }
  }

  private def completeWriteWithCompression(batchId: Long,
                                           newData: java.util.Collection[R],
                                           stream: OutputStream,
                                           header: Header,
                                           metadata: M,
                                           validBatches: Array[Long]): Unit = {
    val map = new util.HashMap[K, RecordWrapper[R]]()
    val _keyProvider = keyProvider
    newData.forEach { r =>
      val rw = new RecordWrapper[R](r, batchId)
      map.put(_keyProvider.getKey(r), rw)
    }
    val recodeSerializer = dataSerializer
    constructState(validBatches, map)
    writeHeadAndMd(stream, header, metadata)
    map.values().forEach(rw => {
      stream.write(recodeSerializer.apply(rw).getBytes(UTF_8))
      stream.write("\n".getBytes(UTF_8))
    })
  }

  private def writeHeadAndMd(stream: OutputStream, header: Header, newMd: M): Unit = {
    stream.write(headerSerializer.apply(header).getBytes(UTF_8))
    stream.write("\n".getBytes(UTF_8))
    stream.write(metadataSerializer.apply(newMd).getBytes(UTF_8))
    stream.write("\n".getBytes(UTF_8))
  }

  protected def headerSerializer: function.Function[Header, String] = h => {
    new ObjectMapper().writeValueAsString(h)
  }

  private def constructState(batchIds: Array[Long],
                             map: util.HashMap[K, RecordWrapper[R]]) = {
    val _keyProvider = keyProvider
    val recodeDeSerializer = dataDeSerializer
    val _retainer = retainer()
    batchIds.foreach { batchId =>
      val fsStream = getFileManager.open(new Path(getPath, batchId.toString))
      val lines = IOSource.fromInputStream(fsStream, UTF_8.name()).getLines()
      val firstLine = lines.next()
      val secondLing = lines.next()
      lines.map(l => {
        recodeDeSerializer(l)
      }).foreach { rw =>
        val r = rw.record
        val key = _keyProvider.getKey(r)
        map.compute(key, (_, oldR) => {
          if (oldR == null || oldR.batchId < rw.batchId) {
            rw
          } else {
            oldR
          }
        })

      }
    }

    // Remove deleted entries
    val it = map.entrySet().iterator()
    while (it.hasNext){
      val n = it.next()
      if(! _retainer.apply(n.getValue.record)) {
        it.remove()
      }
    }

    val m = map.asScala.map { case (key, value) =>
      (key, value)
    }.toMap.asJava
    m
  }

  private def appendWrite(batchId: Long,
                          stream: OutputStream,
                          data: util.Collection[R]): Unit = {
    val _dataSerializer = dataSerializer
    data.iterator().map(new RecordWrapper(_, batchId)).map(_dataSerializer(_)).foreach { f =>
      stream.write(f.getBytes(UTF_8))
      stream.write("\n".getBytes(UTF_8))
    }
  }

  def latestState(): Optional[State[M, R, K]] = synchronized {
    val oLatestBatchId = getLatestBatchId(_latestState.map(_.batchIds.max))
    (oLatestBatchId, _latestState) match {
      case (Some(latestBatchId), Some(ls)) if latestBatchId != ls.batchIds.max =>
        val (header, md) = readHeaderAndMetadata(latestBatchId)
        if (header.validBatches.contains(ls.batchIds.max)) {
          _latestState = Option(loadPartial(ls, latestBatchId))
        } else {
          _latestState = Option(loadComplete(latestBatchId))
        }
      case (Some(latestBatchId), None) =>
        _latestState = Option(loadComplete(latestBatchId))
      case (Some(latestBatchId), Some(ls)) if latestBatchId == ls.batchIds.max =>
      case (None, _) =>
    }
    _latestState match {
      case Some(x) => Optional.of(x)
      case None => Optional.empty()
    }
  }

  def get(k: K): R = {
    val l = latestState()
    val s = if (l.isPresent) Option(l.get()) else None
    s.flatMap(ss => Option.apply(ss.map.get(k))).map(_.record).getOrElse(null.asInstanceOf[R])
  }

  def listAll(): java.lang.Iterable[R] = {
    val l = latestState()
    if (!l.isPresent) {
      return Collections.emptyList()
    }
    l.get().map.asScala.values.map(r => r.record).asJava
  }

  def isEmpty: Boolean = {
    val l = latestState()
    (!l.isPresent) || l.get().map.isEmpty
  }

  protected def getFileManager: CheckpointFileManager

  protected def metadataSerializer: Function[M, String]

  protected def metadataDeSerializer: Function[String, M]

  protected def dataSerializer: Function[RecordWrapper[R], String]

  protected def dataDeSerializer: Function[String, RecordWrapper[R]]

  protected def maxUnCompactedFiles: Int

  protected def getPath: Path

  private def loadComplete(latestBatchId: Long): State[M, R, K] = {
    val path = getPath
    val fileManager = getFileManager
    val fsStream = fileManager.open(new Path(path, latestBatchId.toString))
    val (header, metadata) = headerAndMd(fsStream)
    val map = new util.HashMap[K, RecordWrapper[R]]()
    val m = constructState(header.validBatches, map)
    new State(m, metadata, header.validBatches)
  }

  private def loadPartial(value: State[M, R, K],
                          latestBatchId: Long): State[M, R, K] = {
    val path = getPath
    val fileManager = getFileManager
    val fsStream = fileManager.open(new Path(path, latestBatchId.toString))
    val (header, metadata) = headerAndMd(fsStream)
    val map = new util.HashMap[K, RecordWrapper[R]]()
    value.map.forEach((k, r) => map.put(k, r))
    val m = constructState(header.validBatches.filter(b => !value.batchIds.contains(b)), map)
    new State(m, metadata, header.validBatches)
  }

  private def getLatestBatchId(existing: Option[Long]): Option[Long] = {
    val path = getPath
    val fileManager = getFileManager
    val nextBatchId = existing.map(_ + 1)
    if (existing.nonEmpty &&
      !fileManager.exists(new Path(path, nextBatchId.get.toString))
      && (latestRefreshTime + refreshInterval) > System.currentTimeMillis()) {
      existing
    } else {
      getLatestBatchIdFromListing
    }
  }

  private def headerAndMd(fsStream: InputStream): (Header, M) = {
    val lines = IOSource.fromInputStream(fsStream, UTF_8.name()).getLines()
    val firstLine = lines.next()
    val secondLine = lines.next();
    val header = headerDeSerializer.apply(firstLine)
    val metadata = metadataDeSerializer.apply(secondLine)
    (header, metadata)
  }
}


