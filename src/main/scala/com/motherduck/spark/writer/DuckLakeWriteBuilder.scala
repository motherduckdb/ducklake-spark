package com.motherduck.spark.writer

import com.motherduck.spark.catalog.DuckLakeClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

/**
 * WriteBuilder for DuckLake tables.
 *
 * Creates BatchWrite instances that handle the actual write operation.
 * This is where we control file tracking and DuckLake registration.
 *
 * @param schemaName Schema name (e.g., "main")
 * @param tableName  Table name (without schema prefix)
 * @param schema     Spark schema for the table
 * @param connection DuckLake connection string
 * @param dataPath   Job-specific data path (includes job subfolder)
 * @param client     Shared DuckLakeCatalogClient for metadata operations
 */
class DuckLakeWriteBuilder(
    schemaName: String,
    tableName: String,
    schema: StructType,
    connection: String,
    dataPath: String,
    client: DuckLakeClient
) extends WriteBuilder {

  private val logger = LoggerFactory.getLogger(classOf[DuckLakeWriteBuilder])

  logger.info(s"DuckLakeWriteBuilder created for table: $schemaName.$tableName, dataPath: $dataPath")

  override def build(): Write = {
    new DuckLakeWrite(schemaName, tableName, schema, connection, dataPath, client)
  }
}

/**
 * Write implementation that provides BatchWrite for batch operations.
 */
class DuckLakeWrite(
    schemaName: String,
    tableName: String,
    schema: StructType,
    connection: String,
    dataPath: String,
    client: DuckLakeClient
) extends Write {

  private val logger = LoggerFactory.getLogger(classOf[DuckLakeWrite])

  override def toBatch: BatchWrite = {
    logger.info(s"Creating BatchWrite for table: $schemaName.$tableName")
    new DuckLakeBatchWrite(schemaName, tableName, schema, connection, dataPath, client)
  }

  override def description(): String = s"DuckLakeWrite($schemaName.$tableName)"
}

/**
 * BatchWrite implementation for DuckLake.
 *
 * @param schemaName Schema name (e.g., "main")
 * @param tableName  Table name (without schema prefix)
 * @param schema     Spark schema for the table
 * @param connection DuckLake connection string
 * @param dataPath   Job-specific data path where files will be written
 * @param client     Shared DuckLakeCatalogClient for file registration
 */
class DuckLakeBatchWrite(
    schemaName: String,
    tableName: String,
    schema: StructType,
    connection: String,
    dataPath: String,
    client: DuckLakeClient
) extends BatchWrite {

  private val logger = LoggerFactory.getLogger(classOf[DuckLakeBatchWrite])

  private val fullTableName = s"$schemaName.$tableName"

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    logger.info(s"Creating DataWriterFactory for table: $fullTableName, " +
                s"numPartitions: ${info.numPartitions()}, dataPath: $dataPath")

    // Get Hadoop configuration from active SparkSession (includes S3 credentials etc)
    val hadoopConf = SparkSession.active.sparkContext.hadoopConfiguration

    // Map s3:// scheme to use s3a filesystem implementation
    // DuckLake stores paths with s3:// but Spark/Hadoop uses s3a://
    if (hadoopConf.get("fs.s3.impl") == null) {
      hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    }

    val serializableConf = new SerializableConfiguration(hadoopConf)

    new DuckLakeDataWriterFactory(fullTableName, schema, dataPath, serializableConf)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val commitStartTime = System.currentTimeMillis()
    logger.info(s"BatchWrite.commit() called with ${messages.length} task messages")

    // Collect all files from all tasks
    val allFiles = messages.flatMap {
      case msg: DuckLakeWriterCommitMessage => msg.files
      case null =>
        // Task wrote no data
        Seq.empty
      case other =>
        logger.warn(s"Unexpected commit message type: ${other.getClass}")
        Seq.empty
    }

    logger.info(s"Total files to register: ${allFiles.length}")
    allFiles.foreach(f => logger.debug(s"  File: $f"))

    if (allFiles.nonEmpty) {
      registerFilesWithDuckLake(allFiles)
    } else {
      logger.info("No files to register - write operation complete (no data written)")
    }

    val commitElapsed = System.currentTimeMillis() - commitStartTime
    logger.info(s"TIMING: Commit phase completed in ${commitElapsed}ms (${allFiles.length} files)")
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logger.warn(s"BatchWrite.abort() called with ${messages.length} task messages")

    // Collect files that were written but need cleanup
    val allFiles = messages.flatMap {
      case msg: DuckLakeWriterCommitMessage => msg.files
      case _ => Seq.empty
    }

    if (allFiles.nonEmpty) {
      logger.warn(s"Aborting: ${allFiles.length} files were written but won't be registered")
      // Note: Files remain on storage (S3/local) but are orphaned
      // DuckLake doesn't know about them since we never registered them
      // TODO: clean up the orphaned files via Hadoop FileSystem API
      allFiles.foreach(f => logger.warn(s"  Orphaned file: $f"))
    }
  }

  /**
   * Register written Parquet files with DuckLake catalog.
   *
   * Uses DuckDB JDBC to call ducklake_add_data_files() for each file.
   * Files are registered in the order they were written.
   * Uses the shared client from the catalog.
   */
  private def registerFilesWithDuckLake(files: Seq[String]): Unit = {
    logger.info(s"Registering ${files.length} files with DuckLake table: $fullTableName")

    val registerStartTime = System.currentTimeMillis()
    try {
      // Register files with DuckLake using the shared client
      client.registerDataFiles(schemaName, tableName, files)

      val registerElapsed = System.currentTimeMillis() - registerStartTime
      logger.info(s"TIMING: DuckLake catalog registration completed in ${registerElapsed}ms (${files.length} files)")

    } catch {
      case e: Exception =>
        logger.error(s"Failed to register files with DuckLake: ${e.getMessage}", e)
        throw new RuntimeException(s"DuckLake file registration failed: ${e.getMessage}", e)
    }
    // Note: Don't close the client - it's shared with the catalog
  }
}

/**
 * WriterCommitMessage carries the list of written file paths from executors to driver.
 */
case class DuckLakeWriterCommitMessage(files: Seq[String]) extends WriterCommitMessage
