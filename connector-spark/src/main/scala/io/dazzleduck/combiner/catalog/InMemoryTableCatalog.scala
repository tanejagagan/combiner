package io.dazzleduck.combiner.catalog

import io.dazzleduck.combiner.connector.spark.DDTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.{enumerationAsScalaIteratorConverter, mapAsJavaMapConverter, mapAsScalaMapConverter}

case class InMemoryTable(name : String, columns: Array[Column], partitions: Array[Transform], properties: util.Map[String, String] )

class InMemoryTableCatalog extends TableCatalog with SupportsNamespaces {

  private val inMemoryCatalog = new ConcurrentHashMap[String, (Map[String, String], ConcurrentHashMap[String, InMemoryTable])]()

  var _name : String = _
  private var _catalogPath : String = _
  override def name: String = {
    _name
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this._name = name
    this._catalogPath = options.get("path")
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val lowerStrings = namespace.map(_.toLowerCase)
    val db = inMemoryCatalog.get(lowerStrings(0))
    if(db == null) {
      throw new NoSuchNamespaceException(lowerStrings(0))
    }
    db._2.keys.asScala.map(t => Identifier.of(Array(lowerStrings(0)),t )).toArray
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    val columns = schema.fields.map(f => Column.create(f.name, f.dataType))
    createTable(ident, columns, partitions, properties)
  }

  @throws[TableAlreadyExistsException]
  @throws[NoSuchNamespaceException]
  override def createTable(ident: Identifier, columns: Array[Column], partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    val lowers = ident.namespace().map(_.toLowerCase)
    val db = inMemoryCatalog.get(lowers(0))
    if(db == null ){
      throw new NoSuchNamespaceException(lowers(0))
    }
    val table = db._2.get(ident.name().toLowerCase)
    if(table != null) {
      throw new TableAlreadyExistsException( ident.name())
    }
    val t = InMemoryTable(ident.name(), columns, partitions, properties)
    val path = new Path(getTablePath(ident, properties))
    val fs = path.getFileSystem(SparkSession.active.sessionState.newHadoopConf())
    if(!fs.exists(path)) {
      fs.mkdirs(path)
    }
    db._2.put(ident.name(), t)
    loadTable(ident)
  }

  private def getTablePath(identifier: Identifier, properties: util.Map[String, String]): String = {
    if(properties.containsKey("path") ){
      return properties.get("path")
    }

    if(properties.containsKey("location") ){
      return properties.get("location")
    }

    val db = identifier.namespace().map(_.toLowerCase)
    new Path(new Path(_catalogPath, db(0) ), identifier.name().toLowerCase()).toString
  }

  override def loadTable(ident: Identifier): Table = {
    val lowers = ident.namespace().map(_.toLowerCase)
    val db = inMemoryCatalog.get(lowers(0))
    if(db == null ){
      throw new NoSuchNamespaceException(lowers(0))
    }
    val table = db._2.get(ident.name().toLowerCase)
    if(table == null) {
      throw new NoSuchTableException( ident.name())
    }
    val userSpecifiedSchema =
      Some(new StructType(table.columns.map( c =>
        StructField(c.name, c.dataType))))

    new DDTable(table.name,
      SparkSession.active,
      new CaseInsensitiveStringMap(table.properties),
      Seq(getTablePath(ident, table.properties)),
      userSpecifiedSchema,
      classOf[ParquetFileFormat])

  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  override def dropTable(ident: Identifier): Boolean = {
    val lowers = ident.namespace().map(_.toLowerCase)
    val db = inMemoryCatalog.get(lowers(0))
    if(db == null ){
      throw new NoSuchNamespaceException(lowers(0))
    }
    val table = db._2.get(ident.name().toLowerCase)
    if(table == null) {
      false
    } else {
      db._2.remove(ident.name().toLowerCase)
      true
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???

  override def listNamespaces(): Array[Array[String]] = {
    inMemoryCatalog.keys().asScala.toArray.map(d => Array(d))
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = ???

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    val lower = namespace.map(_.toLowerCase)
    val db = inMemoryCatalog.get(lower(0))
    if(db ==null) {
      throw new NoSuchNamespaceException(namespace)
    }
    inMemoryCatalog.get(lower(0))._1.asJava
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    val lower = namespace.map(_.toLowerCase)
    val db = inMemoryCatalog.get(lower(0))
    inMemoryCatalog.compute(lower(0), (k,v) => {
      if(v==null) {
        (metadata.asScala.toMap, new ConcurrentHashMap[String, InMemoryTable]())
      } else {
        throw new NamespaceAlreadyExistsException(namespace)
      }
    } )
    val path = new Path(getDBPath(namespace))
    val conf = SparkSession.active.sessionState.newHadoopConf()
    val fs = path.getFileSystem( SparkSession.active.sessionState.newHadoopConf())
    fs.mkdirs(path);
  }

  private def getDBPath(namespace : Array[String]) : String = {
    val db = namespace.map(_.toLowerCase)
    val parent = new Path(_catalogPath)
    val dbPath = new Path(db(0));
    new Path(parent, dbPath).toString
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = ???

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = ???
}
