package com.motherduck.spark.stats

import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskStats, WriteTaskStatsTracker}
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * Tracks files written, so they can be registered with the ducklake on commit.
 */
object DuckLakeFileRegistry {
  private val logger = LoggerFactory.getLogger(getClass)

  // Map of jobId -> list of written file paths
  private val registry = new ConcurrentHashMap[String, java.util.List[String]]()

  def registerFile(jobId: String, filePath: String): Unit = {
    registry.computeIfAbsent(jobId, _ => new java.util.concurrent.CopyOnWriteArrayList[String]())
    registry.get(jobId).add(filePath)
    logger.debug(s"Registered file for job $jobId: $filePath")
  }

  def getFiles(jobId: String): Seq[String] = {
    Option(registry.get(jobId))
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty)
  }

  def clearJob(jobId: String): Unit = {
    registry.remove(jobId)
    logger.debug(s"Cleared file registry for job $jobId")
  }

  def getJobCount: Int = registry.size()
}

/**
 * Job-level stats tracker that creates task trackers for each partition.
 */
class DuckLakeStatsTracker(jobId: String) extends WriteJobStatsTracker {
  private val logger = LoggerFactory.getLogger(classOf[DuckLakeStatsTracker])

  logger.info(s"DuckLakeStatsTracker created for job $jobId")

  override def newTaskInstance(): WriteTaskStatsTracker = {
    new DuckLakeTaskStatsTracker(jobId)
  }

  override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
    // Stats are processed in BatchWrite.commit() via the file registry
    logger.debug(s"processStats called with ${stats.size} task stats")
  }
}

/**
 * Task-level stats tracker that records each file as it's written.
 * Runs on executors.
 */
class DuckLakeTaskStatsTracker(jobId: String) extends WriteTaskStatsTracker {
  private val logger = LoggerFactory.getLogger(classOf[DuckLakeTaskStatsTracker])
  private val taskFiles = ArrayBuffer[String]()

  override def newFile(filePath: String): Unit = {
    // This is called when a new output file is created
    logger.debug(s"newFile called: $filePath")

    // Only track .parquet files, skip metadata files
    val fileName = new Path(filePath).getName
    if (fileName.endsWith(".parquet") &&
        !fileName.startsWith("_") &&
        !fileName.startsWith(".")) {
      taskFiles += filePath
      DuckLakeFileRegistry.registerFile(jobId, filePath)
      logger.info(s"Tracked new Parquet file for job $jobId: $filePath")
    }
  }

  override def closeFile(filePath: String): Unit = {
    // Called when a file is closed - no-op
    logger.debug(s"closeFile called: $filePath")
  }

  override def newPartition(partitionValues: org.apache.spark.sql.catalyst.InternalRow): Unit = {
    // Called when writing to a new partition - no-op
    logger.debug("newPartition called")
  }

  override def newRow(filePath: String, row: org.apache.spark.sql.catalyst.InternalRow): Unit = {
    // Called for each row - no-op
  }

  override def getFinalStats(taskCommitTime: Long): WriteTaskStats = {
    logger.info(s"Task completed, tracked ${taskFiles.size} files")
    DuckLakeWriteTaskStats(taskFiles.toSeq)
  }
}

/**
 * Custom stats class to carry file info from tasks back to driver.
 */
case class DuckLakeWriteTaskStats(files: Seq[String]) extends WriteTaskStats
