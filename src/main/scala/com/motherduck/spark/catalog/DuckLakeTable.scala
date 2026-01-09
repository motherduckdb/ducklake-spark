package com.motherduck.spark.catalog

import com.motherduck.spark.writer.DuckLakeWriteBuilder
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

/**
 * Represents an existing DuckLake table in Spark.
 *
 * @param schemaName  Schema name (e.g., "main")
 * @param tableName   Table name (without schema prefix)
 * @param tableSchema Spark StructType schema (queried from DuckLake)
 * @param path        DuckLake metadata catalog connection string
 * @param dataPath    Base path for data files
 * @param client      Shared DuckLakeClient for metadata operations
 */
class DuckLakeTable(
                     val schemaName: String,
                     val tableName: String,
                     val tableSchema: StructType,
                     val path: String,
                     val dataPath: String,
                     val client: DuckLakeClient
) extends Table with SupportsWrite {

  private val logger = LoggerFactory.getLogger(classOf[DuckLakeTable])

  private val fullName = s"$schemaName.$tableName"

  logger.info(s"DuckLakeTable created: $fullName, dataPath=$dataPath")

  override def name(): String = fullName

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_WRITE,
      TableCapability.TRUNCATE
    ).asJava
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    logger.info(s"Creating WriteBuilder for table: $fullName")
    logger.info(s"  Write schema: ${info.schema()}")

    // Generate a unique job path for this write operation
    val jobId = java.util.UUID.randomUUID().toString.take(8)
    val timestamp = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
      .withZone(java.time.ZoneOffset.UTC)
      .format(java.time.Instant.now())

    val jobDataPath = s"$dataPath/job-$timestamp-$jobId"

    new DuckLakeWriteBuilder(schemaName, tableName, info.schema(), path, jobDataPath, client)
  }

  override def toString: String = s"DuckLakeTable($fullName, $dataPath)"
}
