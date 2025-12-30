package com.motherduck.spark.catalog

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import java.util
import scala.jdk.CollectionConverters._

class DuckLakeCatalog extends TableCatalog with SupportsNamespaces {

  private val logger = LoggerFactory.getLogger(classOf[DuckLakeCatalog])

  private var catalogName: String = _
  private var path: String = _
  private var options: CaseInsensitiveStringMap = _
  private var client: DuckLakeClient = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    this.options = options
    this.path = options.get("path")

    logger.info(s"Initializing DuckLakeCatalog '$name' with path: $path")

    if (path == null || path.isEmpty) {
      throw new IllegalArgumentException(
        s"DuckLake catalog '$name' requires 'path' option. " +
        s"Set spark.sql.catalog.$name.path=ducklake:... or spark.sql.catalog.$name.path=md:..."
      )
    }

    this.client = new DuckLakeClient(path)
  }

  override def name(): String = catalogName

  def getPath: String = path

  // ==================== TableCatalog ====================

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val schemaName = namespace.headOption.getOrElse("main")
    logger.info(s"listTables($schemaName)")

    try {
      client.listTables(schemaName)
        .map(tableName => Identifier.of(namespace, tableName))
        .toArray
    } catch {
      case e: Exception =>
        logger.error(s"Failed to list tables in $schemaName: ${e.getMessage}")
        Array.empty
    }
  }

  override def loadTable(ident: Identifier): Table = {
    val schemaName = ident.namespace().headOption.getOrElse("main")
    val tableName = ident.name()
    logger.info(s"loadTable($schemaName.$tableName)")

    // Check if table exists
    if (!client.tableExists(schemaName, tableName)) {
      throw new NoSuchTableException(ident)
    }

    // Get table schema from DuckLake metadata
    val tableSchema = client.getTableSchema(schemaName, tableName)

    // Get data path for this table
    val dataPath = client.getTableDataPath(schemaName, tableName)

    logger.info(s"Loaded table $schemaName.$tableName with ${tableSchema.fields.length} columns, dataPath=$dataPath")

    // Return a DuckLakeTable that supports writing
    new DuckLakeTable(
      tableName = s"$schemaName.$tableName",
      tableSchema = tableSchema,
      path = path,
      dataPath = dataPath,
      client = client
    )
  }

  override def tableExists(ident: Identifier): Boolean = {
    val schemaName = ident.namespace().headOption.getOrElse("main")
    val tableName = ident.name()
    logger.info(s"tableExists($schemaName.$tableName)")

    try {
      client.tableExists(schemaName, tableName)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to check table existence: ${e.getMessage}")
        false
    }
  }

  override def createTable(ident: Identifier,
                           schema: StructType,
                           partitions: Array[Transform],
                           properties: util.Map[String, String]): Table = {
    logger.info(s"createTable(${ident.namespace().mkString(".")}.${ident.name()})")
    // Table creation should be done via DuckDB directly
    // This connector focuses on writing to existing tables
    throw new UnsupportedOperationException(
      "Table creation is not supported via Spark. " +
      "Create tables using DuckDB directly, then use df.writeTo() to write data."
    )
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    logger.info(s"alterTable(${ident.namespace().mkString(".")}.${ident.name()})")
    throw new UnsupportedOperationException(
      "Table alteration is not supported via Spark. Use DuckDB directly."
    )
  }

  override def dropTable(ident: Identifier): Boolean = {
    logger.info(s"dropTable(${ident.namespace().mkString(".")}.${ident.name()})")
    throw new UnsupportedOperationException(
      "Table dropping is not supported via Spark. Use DuckDB directly."
    )
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    logger.info(s"renameTable(${oldIdent.name()} -> ${newIdent.name()})")
    throw new UnsupportedOperationException(
      "Table renaming is not supported via Spark. Use DuckDB directly."
    )
  }

  // ==================== SupportsNamespaces ====================

  override def listNamespaces(): Array[Array[String]] = {
    logger.info("listNamespaces()")

    try {
      client.listSchemas().map(s => Array(s)).toArray
    } catch {
      case e: Exception =>
        logger.error(s"Failed to list namespaces: ${e.getMessage}")
        Array(Array("main"))
    }
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    logger.info(s"listNamespaces(${namespace.mkString(".")})")
    // DuckLake has flat schema structure (no nested namespaces)
    Array.empty
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    logger.info(s"loadNamespaceMetadata(${namespace.mkString(".")})")
    new util.HashMap[String, String]()
  }

  override def createNamespace(namespace: Array[String],
                                metadata: util.Map[String, String]): Unit = {
    logger.info(s"createNamespace(${namespace.mkString(".")})")
    throw new UnsupportedOperationException(
      "Schema creation is not supported via Spark. Use DuckDB directly."
    )
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    logger.info(s"alterNamespace(${namespace.mkString(".")})")
    throw new UnsupportedOperationException(
      "Schema alteration is not supported via Spark. Use DuckDB directly."
    )
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    logger.info(s"dropNamespace(${namespace.mkString(".")}, cascade=$cascade)")
    throw new UnsupportedOperationException(
      "Schema dropping is not supported via Spark. Use DuckDB directly."
    )
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    logger.info(s"namespaceExists(${namespace.mkString(".")})")

    if (namespace.length != 1) return false

    try {
      client.listSchemas().contains(namespace(0))
    } catch {
      case e: Exception =>
        logger.error(s"Failed to check namespace existence: ${e.getMessage}")
        namespace(0) == "main"  // Assume main exists
    }
  }
}
