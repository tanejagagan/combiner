package io.dazzleduck.combiner.catalog

import io.dazzleduck.combiner.catalog.AppendOnlyFileSystemCommitter.Result
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.catalog.Column
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.json4s.Extraction.extract
import org.json4s.JsonAST.JValue
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.{Serialization, parseJson}
import org.json4s.jackson.JsonMethods._

import java.lang
import java.util.function

case class CatalogMd(batchId : Long,
                     updates : Map[String, Long])

trait CatalogObject {
  def action : String
  def `type` : String
  def id : String
  def description : String
  def properties : Map[String, String]
  def withDeleteAction : CatalogObject
}

object CatalogObject {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)
  val objectTypeMap : Map[Class[_], String] = Map(classOf[CatalogTable] -> "table",
    classOf[CatalogNamespace] -> "namespace",
    classOf[CatalogView] -> "view",
    classOf[CatalogFunction] -> "function")

  def jsonExtract(str : String) : CatalogObject = {
    jsonExtract(parse(str))
  }

  def jsonExtract(obj : JValue) : CatalogObject = {
    extract[String](obj \ "type") match {
      case "table" => extract[CatalogTable](obj)
      case "view" => extract[CatalogView](obj)
      case "namespace" => extract[CatalogNamespace](obj)
      case "function" => extract[CatalogFunction](obj)
    }
  }
}
case class CatalogColumn (name: String,
                          dataType: String,
                          nullable: Boolean,
                          comment: String) {
  def this(c : Column) = this(c.name(), c.dataType().sql, c.nullable(), c.comment())
}


case class CatalogTable(name : String,
                        namespace : Seq[String],
                        columns : Seq[CatalogColumn],
                        transforms : Seq[String],
                        description : String,
                        properties : Map[String, String],
                        action : String,
                        `type` : String = "table"  ) extends CatalogObject {
  def id = name

  override def withDeleteAction: CatalogObject = copy(action = Actions.DELETE.toString)
}
case class CatalogNamespace( namespace : Seq[String],
                             path : String,
                             description : String,
                             properties : Map[String, String],
                             action : String,
                             `type` : String = "namespace") extends CatalogObject {
  def id : String = namespace.mkString(".")

  override def withDeleteAction: CatalogObject = copy(action = Actions.DELETE.toString)
}
case class CatalogView( name : String,
                        namespace : Seq[String],
                        description : String,
                        properties : Map[String, String],
                        action: String,
                        `type` : String = "view") extends CatalogObject {
  def id : String = (namespace :+ name).mkString(".")

  override def withDeleteAction: CatalogObject = copy(action = Actions.DELETE.toString)
}
case class CatalogFunction ( name : String,
                             namespace :Seq[String],
                             description : String,
                             properties : Map[String, String],
                             action: String,
                             `type` : String = "function" ) extends CatalogObject {
  def id : String = (namespace :+ name).mkString(".")

  override def withDeleteAction: CatalogObject = copy(action = Actions.DELETE.toString)
}

case class CatalogObjectRecordWrapper(record : CatalogObject, batchId : Long)

class CatalogFileSystemCommitter (pathString : String,
                                  hadoopConfiguration : Configuration)
  extends AbstractAppendOnlyFileSystemCommitter[CatalogMd, CatalogObject, String] {

  import org.json4s._

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  val path = new Path(pathString)

  val checkpointFileManager = CheckpointFileManager.create(path, hadoopConfiguration)

  if(!checkpointFileManager.exists(path)) {
    checkpointFileManager.mkdirs(path)
  }

  override protected def getFileManager: CheckpointFileManager = checkpointFileManager

  override protected def metadataSerializer: function.Function[CatalogMd, String] = Serialization.write(_)

  override protected def metadataDeSerializer: function.Function[String, CatalogMd] = str => {
    Serialization.read[CatalogMd](str)
  }

  override protected def dataSerializer: function.Function[RecordWrapper[CatalogObject], String] = wr => {
    Serialization.write[CatalogObjectRecordWrapper](CatalogObjectRecordWrapper(wr.record, wr.batchId))
  }

  override protected def dataDeSerializer: function.Function[String, RecordWrapper[CatalogObject]] = str => {
    val json = parse(str)
    val record = CatalogObject.jsonExtract(json \ "record")
    val batchId = extract[Long](json \ "batchId")
    new RecordWrapper(record, batchId)
  }

  override protected def maxUnCompactedFiles: Int = 20

  override protected def getPath: Path = path

  override def keyProvider(): KeyProvider[String, CatalogObject] = o => o.id

  override def retainer(): function.Function[CatalogObject, lang.Boolean] = o => {
    o.action != Actions.DELETE.toString
  }

  override def proceed(batchId: Long, oldMetadata: CatalogMd, newMetadata: CatalogMd): Result = {
    if(newMetadata.updates.forall{ case(k, b) => oldMetadata.updates.getOrElse(k, 0l) <= b}) {
      Result.OK
    } else {
      Result.CONCURRENT_MODIFICATION
    }
  }

  override def resolveMetaData(batchId: Long, oldMetadata: CatalogMd, newMetadata: CatalogMd): CatalogMd = {
    val newMap = newMetadata.updates.map{case(k, _) => (k, batchId)}
    val oldMap = if(oldMetadata == null ) {
      Map()
    } else {
      oldMetadata.updates
    }
    CatalogMd(batchId, oldMap ++ newMap)
  }

  def add(r : CatalogObject): Unit = {
    assert(r.action == Actions.ADD.toString, "action should be add")
    val batchId = getLatestBatchIdFromListing.map(_ + 1).getOrElse(0L)
    append(r, new CatalogMd(batchId, Map(r.id -> batchId)))
  }

  def delete(r: CatalogObject): Unit = {
    val toDelete = r.withDeleteAction
    append(toDelete, new CatalogMd(0, Map(r.id -> Long.MaxValue)))
  }
}
