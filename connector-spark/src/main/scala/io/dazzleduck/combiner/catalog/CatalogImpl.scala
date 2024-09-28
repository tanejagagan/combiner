package io.dazzleduck.combiner.catalog

import io.dazzleduck.combiner.catalog.CatalogImpl.DEFAULT_NAMESPACE
import io.dazzleduck.combiner.connector.spark.DDTable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.JavaConverters._


object CatalogImpl {
  val DEFAULT_PATH = "dd-catalog"
  val DEFAULT_NAMESPACE = Array("default")
}
class CatalogImpl extends TableCatalog with ViewCatalog with SupportsNamespaces {

  private val readWriteLock = new ReentrantReadWriteLock();
  private var _name : String = _
  private var _path : String = _
  private var _catalogDAO : CatalogFileSystemCommitter = _
  private var _options : CaseInsensitiveStringMap = _

  override def name: String = {
    _name
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap) : Unit = {
    var path = options.get("path")
    if(path == null)
      path = new Path(System.getProperty("user.dir"), CatalogImpl.DEFAULT_PATH).toString
    _name = name
    _path = path
    _catalogDAO = new CatalogFileSystemCommitter(new Path(_path, "_metadata").toString, SparkSession.active.sessionState.newHadoopConf())
    _options = options
    if(!this.namespaceExists(DEFAULT_NAMESPACE)){
      createNamespace(DEFAULT_NAMESPACE, Collections.emptyMap())
    }
  }

  private val namespaceDAOMap = new ConcurrentHashMap[String, CatalogFileSystemCommitter]()


  private def getNamespaceDAO(namespace : Array[String]) : CatalogFileSystemCommitter = {
    val key = makeQualified(namespace)
    if(!_catalogDAO.latestState().isPresent) {
      namespaceDAOMap.remove(key)
      return null;
    }
    namespaceDAOMap.compute( key, (k, old) => {
      if(old != null){
        old
      } else {
        val latestState = _catalogDAO.latestState()
        if(! latestState.isPresent) {
          return null
        } else {
          val l = latestState.get()
          val path = l.map.get(key).record.asInstanceOf[CatalogNamespace].path
          new CatalogFileSystemCommitter(new Path(path, "_metadata").toString, SparkSession.active.sessionState.newHadoopConf())
        }
      }
    })
  }

  private def validateNamespace(strings: Array[String]): Unit = {

  }

  private def getNamespacePath(namespace : Array[String]) : String = {
    val key = makeQualified(namespace)
    new Path(_path, key).toString
  }

  private def createPath(path : String,
                         configuration: Configuration ) : Unit = {
    val fs = new Path(path).getFileSystem(configuration)
    fs.mkdirs(new Path(path))
  }

  private def deletePath(path : String, configuration: Configuration ) : Unit = {
    val fs = new Path(path).getFileSystem(configuration)
    fs.delete(new Path(path), true)
  }

  private def getTablePath(identifier: Identifier, properties: util.Map[String, String]): String = {
    if(properties.containsKey("path") ){
      return properties.get("path")
    }

    if(properties.containsKey("location") ){
      return properties.get("location")
    }

    val db = identifier.namespace().map(_.toLowerCase)
    new Path(new Path(_path, db(0) ), identifier.name().toLowerCase()).toString
  }

  private def makeQualified(strings : Array[String]) : String = {
    strings.mkString(".")
  }

  override def createNamespace(strings: Array[String],
                               map: util.Map[String, String]): Unit = {
    val lowerStrings = strings.map(_.toLowerCase)
    validateNamespace(lowerStrings)
    val key = makeQualified(lowerStrings)
    withWriteLock {
      val current = Option.apply(_catalogDAO.get(key))
      current match {
        case Some(x) => throw new NamespaceAlreadyExistsException(lowerStrings)
        case None =>
          val path = getNamespacePath(lowerStrings)
          createPath(path, SparkSession.active.sessionState.newHadoopConf())
          val co = new CatalogNamespace(lowerStrings, path, "", map.asScala.toMap, Actions.ADD.toString )
          _catalogDAO.add(co)

      }
    }
  }

  override def alterNamespace(strings: Array[String], namespaceChanges: NamespaceChange*): Unit = {
    val lowerStrings = strings.map(_.toLowerCase)
    withWriteLock {
      val key = makeQualified(strings)
      ???
    }
  }

  override def dropNamespace(strings: Array[String], cascade: Boolean): Boolean = {
    val lowerStrings = strings.map(_.toLowerCase)
    if(cascade) {
      throw new UnsupportedOperationException("cascade is not supported")
    }
    withWriteLock {
      val key = makeQualified(lowerStrings)
      val co = Option.apply(_catalogDAO.get(key))
      co match {
        case Some(o) =>
          val nsCommitter = getNamespaceDAO(lowerStrings)
          if(nsCommitter.isEmpty) {
            _catalogDAO.delete(o)
            deletePath(o.asInstanceOf[CatalogNamespace].path, SparkSession.active.sessionState.newHadoopConf())
          }
          true
        case None => throw new NoSuchNamespaceException(strings)
      }
    }
  }

  override def listTables(strings: Array[String]): Array[Identifier] = {
    val lowerStrings = strings.map(_.toLowerCase)
    val key = makeQualified(lowerStrings)
    withReadLock {
      val co = Option.apply(_catalogDAO.get(key))
      co match {
        case Some(o) =>
          val nsCommitter = getNamespaceDAO(lowerStrings)
          nsCommitter.listAll().asScala
            .filter(x => x.isInstanceOf[CatalogTable]).
            map(x => Identifier.of(lowerStrings,x.asInstanceOf[CatalogTable].name )).toArray
        case None => throw new NoSuchNamespaceException(lowerStrings)
      }
    }
  }

  override def loadTable(identifier: Identifier): Table = {
    val time = System.currentTimeMillis()
    loadTable(identifier, time * 1000)
  }

  override def loadTable(ident: Identifier, timestamp: Long): Table = {

    // Only identity transformation is supported
    def getPartitionSchema(columns : Seq[CatalogColumn], transformations : Seq[String]) : StructType = {
      val map = columns.flatMap{c =>
        Map(c.name -> new StructField(c.name, DataType.fromDDL(c.dataType)),
          s"identity(${c.name})" -> new StructField(c.name, DataType.fromDDL(c.dataType))
        )
      }.toMap
      new StructType( transformations.map(t => map(t)).toArray)
    }

    val lowerNamespace = ident.namespace().map(_.toLowerCase)
    withReadLock {
      val key = makeQualified(lowerNamespace)
      val co = Option.apply(_catalogDAO.get(key))
      co match {
        case Some(o) =>
          val nsDAO = getNamespaceDAO(lowerNamespace)
          val t =  Option.apply(nsDAO.get(ident.name().toLowerCase))
          t match {
            case Some(table : CatalogTable) =>
              val userSpecifiedSchema =
                Some(new StructType(table.columns.map( c =>
                  StructField(c.name, DataType.fromDDL(c.dataType))).toArray))

              new DDTable(table.name,
                SparkSession.active,
                new CaseInsensitiveStringMap(table.properties.asJava),
                Seq(getTablePath(ident, table.properties.asJava)),
                userSpecifiedSchema,
                classOf[ParquetFileFormat])
            case None => throw new NoSuchTableException(ident)
          }

        case None => throw new NoSuchNamespaceException(ident.namespace())
      }
    }
  }

  override def loadTable(ident: Identifier, version: String): Table = {
    throw new UnsupportedOperationException("table with the version is not supported")
  }

  override def createTable(identifier: Identifier,
                           structType: StructType,
                           transforms: Array[Transform],
                           map: util.Map[String, String]): Table = {
    val columns = structType.fields.map(f => Column.create(f.name, f.dataType))
    createTable(identifier, columns, transforms, map )
  }

  @throws[TableAlreadyExistsException]
  @throws[NoSuchNamespaceException]
  override def createTable(ident: Identifier,
                           columns: Array[Column],
                           partitions: Array[Transform],
                           properties: util.Map[String, String]): Table = {
    val namespace = ident.namespace();
    val name = ident.name().toLowerCase
    val key = makeQualified(namespace)

    if (!partitions.forall{ c =>c.name() == "identity"}) {
      throw new RuntimeException("only identity transformation is supported")
    }

    if (partitions.nonEmpty && partitions.forall( t => t.name() != "identity")) {
      throw new IllegalArgumentException("only identity is supported in partitions")
    }
    val lowerNamespace = ident.namespace().map(_.toLowerCase)
    withWriteLock {
      val n = getNamespaceDAO(lowerNamespace)
      if(n == null) {
        throw new NoSuchNamespaceException(key)
      }
      val actualPath = getTablePath(ident, properties)
      val p = new util.HashMap[String, String](properties)
      p.put("path", actualPath)
      val catalogTable = CatalogTable(name, ident.namespace(), columns.map(c => new CatalogColumn(c)),
        partitions.map(_.toString), "", p.asScala.toMap, Actions.ADD.toString)
      n.add(catalogTable)
      createPath(actualPath, SparkSession.active.sessionState.newHadoopConf())
      loadTable(ident)
    }
  }

  override def alterTable(identifier: Identifier, tableChanges: TableChange*): Table = ???

  override def dropTable(identifier: Identifier): Boolean = {
    val lowerStrings = identifier.namespace().map(_.toLowerCase)
    val key = makeQualified(lowerStrings)
    withReadLock {
      val co = Option.apply(_catalogDAO.get(key))
      co match {
        case Some(_) =>
          val nsCommitter = getNamespaceDAO(lowerStrings)
          val table = nsCommitter.get(identifier.name())
          if(table == null) {
            return false
          } else {
            nsCommitter.delete(table)
            return true
          }
        case None => throw new NoSuchNamespaceException(lowerStrings)
      }
    }
  }

  override def renameTable(identifier: Identifier, identifier1: Identifier): Unit = ???

  override def listViews(strings: String*): Array[Identifier] = ???

  override def loadView(identifier: Identifier): View = ???

  override def createView(identifier: Identifier, s: String, s1: String, strings: Array[String],
                          structType: StructType, strings1: Array[String],
                          strings2: Array[String], strings3: Array[String], map: util.Map[String, String]): View = ???

  override def alterView(identifier: Identifier, viewChanges: ViewChange*): View = ???

  override def dropView(identifier: Identifier): Boolean = ???

  override def renameView(identifier: Identifier, identifier1: Identifier): Unit = ???

  private def withReadLock[T]( fn  : => T ) : T = {
    val lock = readWriteLock.readLock()
    try {
      lock.lock()
      fn
    } finally {
      lock.unlock()
    }
  }

  private def withWriteLock[T](fn : => T ) : T = {
    val lock = readWriteLock.writeLock();
    try {
      lock.lock()
      fn
    } finally {
      lock.unlock()
    }
  }

  override def listNamespaces(): Array[Array[String]] = {
    withReadLock {
      _catalogDAO.listAll().asScala.map{ c =>
        val ns = c.asInstanceOf[CatalogNamespace]
        ns.namespace.toArray
      }.toArray
    }
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    validateNamespace(namespace)
    val qualified = makeQualified(namespace)
    withReadLock {
      namespaceDAOMap.keySet().stream().filter( e => e== qualified).toArray( size => new Array[Array[String]](size))
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    validateNamespace(namespace)
    val key = makeQualified(namespace)
    withReadLock {
      val ns = Option.apply(_catalogDAO.get(key))
      ns match {
        case Some(n) => n.properties.asJava
        case None => throw new NoSuchNamespaceException(namespace)
      }
    }
  }
}