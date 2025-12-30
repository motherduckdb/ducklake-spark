package com.motherduck.spark.catalog

import org.apache.spark.sql.types._
import org.duckdb.{DuckDBColumnType, DuckDBResultSetMetaData}
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager}
import java.util.Properties

/**
 * Client for managing all ducklake catalog operations.
 * Uses DuckDB ducklake extension rather than direct metadata manipulation.
 *
 * @param ducklakePath DuckLake metadata catalog path (local duckdb, MotherDuck, Postgres etc)
 */
class DuckLakeClient(val ducklakePath: String) extends AutoCloseable {

  private val logger = LoggerFactory.getLogger(classOf[DuckLakeClient])

  // Lazy connection - only created when needed
  private var jdbcConnection: Connection = _
  private val catalogAlias = "ducklake_catalog"

  private def getConnection: Connection = {
    if (jdbcConnection == null || jdbcConnection.isClosed) {
      Class.forName("org.duckdb.DuckDBDriver")

      val props = new Properties()
      props.setProperty("custom_user_agent", "md-spark")

      // Connect to an in-memory DuckDB that will attach to the DuckLake catalog
      jdbcConnection = DriverManager.getConnection("jdbc:duckdb:", props)

      // Install and load DuckLake extension
      val stmt = jdbcConnection.createStatement()
      try {
        stmt.execute("INSTALL ducklake")
        stmt.execute("LOAD ducklake")

        val attachSql = s"ATTACH '$ducklakePath' AS $catalogAlias"
        logger.info(s"Attaching DuckLake catalog: $attachSql")
        stmt.execute(attachSql)
      } finally {
        stmt.close()
      }
    }
    jdbcConnection
  }

  def getTableSchema(schemaName: String, tableName: String): StructType = {
    val conn = getConnection
    val stmt = conn.prepareStatement(
      """SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale
        |FROM duckdb_columns()
        |WHERE database_name = ?
        |  AND schema_name = ?
        |  AND table_name = ?
        |ORDER BY column_index""".stripMargin)

    try {
      stmt.setString(1, catalogAlias)
      stmt.setString(2, schemaName)
      stmt.setString(3, tableName)

      logger.info(s"Querying schema for $catalogAlias.$schemaName.$tableName via duckdb_columns()")
      val rs = stmt.executeQuery()

      val fields = scala.collection.mutable.ArrayBuffer[StructField]()
      while (rs.next()) {
        val columnName = rs.getString("column_name")
        val dataType = rs.getString("data_type")
        val isNullable = rs.getString("is_nullable") == "YES"
        val precision = rs.getObject("numeric_precision")
        val scale = rs.getObject("numeric_scale")

        val sparkType = duckDbTypeToSparkType(
          dataType,
          Option(precision).map(_.asInstanceOf[Number].intValue()),
          Option(scale).map(_.asInstanceOf[Number].intValue())
        )
        fields += StructField(columnName, sparkType, isNullable)
      }
      rs.close()

      if (fields.isEmpty) {
        throw new RuntimeException(s"Table not found: $schemaName.$tableName in catalog $catalogAlias")
      }

      StructType(fields.toArray)
    } finally {
      stmt.close()
    }
  }

  /**
   * Get the data path for a table from DuckLake catalog metadata.
   *
   * The data path is constructed from the catalog's global data_path setting
   * plus the schema and table name: {data_path}/{schema}/{table}
   *
   * @param schemaName DuckLake schema name
   * @param tableName  Table name
   * @return Base path for data files
   */
  def getTableDataPath(schemaName: String, tableName: String): String = {
    val conn = getConnection
    val stmt = conn.createStatement()

    try {
      // Query the internal ducklake_metadata table for data_path
      val metadataTable = s"__ducklake_metadata_$catalogAlias.ducklake_metadata"
      val sql = s"SELECT value FROM $metadataTable WHERE key = 'data_path' AND scope IS NULL"
      logger.info(s"Querying data_path: $sql")
      val rs = stmt.executeQuery(sql)

      if (rs.next()) {
        val basePath = rs.getString("value").stripSuffix("/")
        rs.close()
        // Construct full path: basePath/schema/table
        val fullPath = s"$basePath/$schemaName/$tableName"
        logger.info(s"Resolved data path for $schemaName.$tableName: $fullPath")
        fullPath
      } else {
        rs.close()
        throw new RuntimeException(s"Could not determine data path for $schemaName.$tableName - data_path not found in ducklake_metadata")
      }
    } finally {
      stmt.close()
    }
  }


  def tableExists(schemaName: String, tableName: String): Boolean = {
    val conn = getConnection
    val stmt = conn.prepareStatement(
      """SELECT 1 FROM duckdb_tables()
        |WHERE database_name = ?
        |  AND schema_name = ?
        |  AND table_name = ?""".stripMargin)

    try {
      stmt.setString(1, catalogAlias)
      stmt.setString(2, schemaName)
      stmt.setString(3, tableName)

      val rs = stmt.executeQuery()
      val exists = rs.next()
      rs.close()
      exists
    } finally {
      stmt.close()
    }
  }

  def listTables(schemaName: String): Seq[String] = {
    val conn = getConnection
    val stmt = conn.prepareStatement(
      """SELECT table_name FROM duckdb_tables()
        |WHERE database_name = ?
        |  AND schema_name = ?""".stripMargin)

    try {
      stmt.setString(1, catalogAlias)
      stmt.setString(2, schemaName)

      val rs = stmt.executeQuery()
      val tables = scala.collection.mutable.ArrayBuffer[String]()
      while (rs.next()) {
        tables += rs.getString("table_name")
      }
      rs.close()
      tables.toSeq
    } finally {
      stmt.close()
    }
  }

  def listSchemas(): Seq[String] = {
    val conn = getConnection
    val stmt = conn.prepareStatement(
      """SELECT schema_name FROM duckdb_schemas()
        |WHERE database_name = ?""".stripMargin)

    try {
      stmt.setString(1, catalogAlias)

      val rs = stmt.executeQuery()
      val schemas = scala.collection.mutable.ArrayBuffer[String]()
      while (rs.next()) {
        schemas += rs.getString("schema_name")
      }
      rs.close()
      schemas.toSeq
    } finally {
      stmt.close()
    }
  }

  /**
   * Register data files with DuckLake catalog after a write operation.
   *
   * @param schemaName Schema name
   * @param tableName  Table name (just the name, not schema.table)
   * @param filePaths  List of Parquet file paths to register
   */
  def registerDataFiles(schemaName: String, tableName: String, filePaths: Seq[String]): Unit = {
    val conn = getConnection
    val stmt = conn.createStatement()

    try {
      for (filePath <- filePaths) {
        // ducklake_add_data_files expects: catalog_name, table_name, file_path
        // Note: table_name should be just the name, not schema.table
        val sql = s"CALL ducklake_add_data_files('$catalogAlias', '$tableName', '$filePath')"
        logger.info(s"Registering file: $sql")
        stmt.execute(sql)
      }
    } finally {
      stmt.close()
    }
  }

  /**
   * Convert a DuckDB type to Spark DataType using DuckDBColumnType enum.
   *
   * @param duckDbType DuckDB type string (e.g., "VARCHAR", "INTEGER", "DECIMAL(10,2)")
   * @param precision  Optional precision from duckdb_columns() metadata
   * @param scale      Optional scale from duckdb_columns() metadata
   */
  def duckDbTypeToSparkType(duckDbType: String,
                            precision: Option[Int] = None,
                            scale: Option[Int] = None): DataType = {
    // Use duckdb-java's type conversion
    val columnType = DuckDBResultSetMetaData.TypeNameToType(duckDbType)

    columnType match {
      // Boolean
      case DuckDBColumnType.BOOLEAN => BooleanType

      // Integer types
      case DuckDBColumnType.TINYINT => ByteType
      case DuckDBColumnType.SMALLINT => ShortType
      case DuckDBColumnType.INTEGER => IntegerType
      case DuckDBColumnType.BIGINT => LongType

      // Unsigned integers (map to next larger signed type)
      case DuckDBColumnType.UTINYINT => ShortType
      case DuckDBColumnType.USMALLINT => IntegerType
      case DuckDBColumnType.UINTEGER => LongType
      case DuckDBColumnType.UBIGINT => DecimalType(20, 0)  // UBIGINT can exceed Long.MAX_VALUE

      // Large integers
      case DuckDBColumnType.HUGEINT => DecimalType(38, 0)
      case DuckDBColumnType.UHUGEINT => DecimalType(38, 0)

      // Floating point
      case DuckDBColumnType.FLOAT => FloatType
      case DuckDBColumnType.DOUBLE => DoubleType

      // Decimal - use precision/scale from metadata when available
      case DuckDBColumnType.DECIMAL =>
        val p = precision.getOrElse(parseDecimalPrecision(duckDbType))
        val s = scale.getOrElse(parseDecimalScale(duckDbType))
        DecimalType(p, s)

      // String types
      case DuckDBColumnType.VARCHAR => StringType
      case DuckDBColumnType.BLOB => BinaryType
      case DuckDBColumnType.UUID => StringType  // UUIDs as strings in Spark
      case DuckDBColumnType.JSON => StringType  // JSON as strings in Spark

      // Date/Time types
      case DuckDBColumnType.DATE => DateType
      case DuckDBColumnType.TIME => StringType  // Spark doesn't have TimeType
      case DuckDBColumnType.TIME_WITH_TIME_ZONE => StringType
      case DuckDBColumnType.TIMESTAMP => TimestampType
      case DuckDBColumnType.TIMESTAMP_S => TimestampType
      case DuckDBColumnType.TIMESTAMP_MS => TimestampType
      case DuckDBColumnType.TIMESTAMP_NS => TimestampType  // Note: Spark Timestamp has microsecond precision
      case DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE => TimestampType
      case DuckDBColumnType.INTERVAL => StringType  // Spark CalendarIntervalType is internal

      // Other
      case DuckDBColumnType.BIT => BinaryType
      case DuckDBColumnType.ENUM => StringType  // Enums as strings
      case DuckDBColumnType.UNION => StringType  // Unions as strings (simplified)

      // TODO: list/array/etc

      case DuckDBColumnType.UNKNOWN | _ =>
        logger.warn(s"Unknown DuckDB type: $duckDbType, defaulting to StringType")
        StringType
    }
  }

  /**
   * Parse precision from DECIMAL(precision, scale) type string.
   * TODO: I had a better way in the fivetran connector somewhere
   */
  private def parseDecimalPrecision(typeStr: String): Int = {
    val pattern = """DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)""".r
    typeStr match {
      case pattern(precision, _) => precision.toInt
      case _ => 38  // Default precision
    }
  }

  /**
   * Parse scale from DECIMAL(precision, scale) type string.
   * TODO: same, I had a better way in the fivetran connector somewhere
   */
  private def parseDecimalScale(typeStr: String): Int = {
    val pattern = """DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)""".r
    typeStr match {
      case pattern(_, scale) => scale.toInt
      case _ => 18  // Default scale
    }
  }

  override def close(): Unit = {
    if (jdbcConnection != null && !jdbcConnection.isClosed) {
      jdbcConnection.close()
    }
  }
}
