package com.motherduck.spark.writer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{TaskAttemptContext, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.parquet.{ParquetOutputWriter, ParquetWriteSupport}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Factory that creates DataWriters on executors.
 *
 * This is serialized and sent to each executor where it creates
 * task-specific DataWriter instances.
 */
class DuckLakeDataWriterFactory(
    tableName: String,
    schema: StructType,
    dataPath: String
) extends DataWriterFactory with Serializable {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[DuckLakeDataWriterFactory])

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    logger.info(s"Creating DataWriter for partition=$partitionId, task=$taskId, dataPath=$dataPath")
    new DuckLakeDataWriter(tableName, schema, dataPath, partitionId, taskId)
  }
}

/**
 * DataWriter that writes rows to Parquet files.
 *
 * Maximum reuse of existing spark/hadoop functionality for writing Prquet.
 * Runs on executors. Each task gets its own DataWriter instance.
 * Tracks all files written and returns them in commit().
 */
class DuckLakeDataWriter(
    tableName: String,
    schema: StructType,
    dataPath: String,
    partitionId: Int,
    taskId: Long
) extends DataWriter[InternalRow] {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[DuckLakeDataWriter])

  // Track files written by this task
  private val writtenFiles = ArrayBuffer[String]()

  // Current Parquet writer
  private var currentWriter: OutputWriter = _
  private var currentFilePath: String = _
  private var rowCount: Long = 0
  private var totalRowCount: Long = 0

  // Configuration
  private val maxRowsPerFile: Long = 1000000  // 1M rows per file

  /**
   * Create Hadoop configuration with all settings required for ParquetWriteSupport.
   * These settings are normally set by ParquetFileFormat.prepareWrite() via SparkSession,
   * but we set them manually since we don't have access to SparkSession on executors.
   */
  private def createParquetConfiguration(): Configuration = {
    val hadoopConf = new Configuration()

    // Set the schema for ParquetWriteSupport
    ParquetWriteSupport.setSchema(schema, hadoopConf)

    // Required settings - ParquetWriteSupport.init() asserts these are not null
    hadoopConf.set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, "false")
    hadoopConf.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key, "INT96")
    hadoopConf.set(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key, "false")

    // Set compression
    hadoopConf.set(ParquetOutputFormat.COMPRESSION, "SNAPPY")
    hadoopConf.set(ParquetOutputFormat.BLOCK_SIZE, (128 * 1024 * 1024).toString)

    // Set the write support class
    hadoopConf.set(ParquetOutputFormat.WRITE_SUPPORT_CLASS, classOf[ParquetWriteSupport].getName)

    hadoopConf
  }

  override def write(row: InternalRow): Unit = {
    // Start new file if needed
    if (currentWriter == null || shouldRollFile()) {
      rollToNewFile()
    }

    currentWriter.write(row)
    rowCount += 1
    totalRowCount += 1
  }

  private def shouldRollFile(): Boolean = {
    rowCount >= maxRowsPerFile
  }

  private def rollToNewFile(): Unit = {
    // Close current file if open
    closeCurrentFile()

    // Generate unique file path
    val timestamp = System.currentTimeMillis()
    val uuid = java.util.UUID.randomUUID().toString.take(8)
    val fileName = s"part-$partitionId-$taskId-$timestamp-$uuid.snappy.parquet"
    currentFilePath = s"$dataPath/$fileName"

    logger.info(s"Starting new Parquet file: $currentFilePath")

    writtenFiles += currentFilePath
    rowCount = 0

    // Create properly configured Hadoop configuration
    val hadoopConf = createParquetConfiguration()

    // Create task attempt context
    val taskAttemptId = new TaskAttemptID(
      new org.apache.hadoop.mapreduce.TaskID(
        new org.apache.hadoop.mapreduce.JobID("ducklake", partitionId.toInt),
        TaskType.MAP,
        taskId.toInt
      ),
      0
    )
    val taskContext = new TaskAttemptContextImpl(hadoopConf, taskAttemptId)

    // Create ParquetOutputWriter directly with our configured context
    currentWriter = new ParquetOutputWriter(currentFilePath, taskContext)
  }

  private def closeCurrentFile(): Unit = {
    if (currentWriter != null) {
      logger.debug(s"Closing file: $currentFilePath with $rowCount rows")
      currentWriter.close()
      currentWriter = null
      currentFilePath = null
    }
  }

  override def commit(): WriterCommitMessage = {
    closeCurrentFile()
    logger.info(s"Task $partitionId-$taskId commit: wrote ${writtenFiles.size} files, $totalRowCount total rows")

    // Return file list to driver
    DuckLakeWriterCommitMessage(writtenFiles.toSeq)
  }

  override def abort(): Unit = {
    logger.warn(s"Task $partitionId-$taskId abort: ${writtenFiles.size} files written but not registered")
    closeCurrentFile()

    // Note: Files remain on storage but won't be registered with DuckLake
    writtenFiles.foreach { path =>
      logger.warn(s"Orphaned file: $path")
    }
  }

  override def close(): Unit = {
    closeCurrentFile()
  }
}
