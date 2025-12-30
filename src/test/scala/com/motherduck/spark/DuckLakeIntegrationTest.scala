package com.motherduck.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}
import java.sql.DriverManager
import java.util.Comparator

class DuckLakeIntegrationTest extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: Path = _
  private var catalogPath: String = _
  private var dataPath: String = _

  override def beforeAll(): Unit = {
    // Create temp directory for test
    tempDir = Files.createTempDirectory("ducklake-spark-test")
    catalogPath = tempDir.resolve("test.ducklake").toString
    dataPath = tempDir.resolve("data").toString

    // Create DuckLake catalog and table using pure DuckDB JDBC
    createDuckLakeCatalog()
  }

  override def afterAll(): Unit = {
    // Clean up temp directory
    if (tempDir != null) {
      Files.walk(tempDir)
        .sorted(Comparator.reverseOrder())
        .forEach(Files.delete(_))
    }
  }

  private def createDuckLakeCatalog(): Unit = {
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")

    try {
      val stmt = conn.createStatement()
      try {
        // Install and load DuckLake extension
        stmt.execute("INSTALL ducklake")
        stmt.execute("LOAD ducklake")

        // Create a new DuckLake catalog with data files in subdirectory
        stmt.execute(
          s"""ATTACH 'ducklake:$catalogPath' AS test_lake (
             |  DATA_PATH '$dataPath'
             |)""".stripMargin)

        // Create test table
        stmt.execute(
          """CREATE TABLE test_lake.main.users (
            |  id INTEGER,
            |  name VARCHAR,
            |  age INTEGER
            |)""".stripMargin)

      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }
  }

  test("write rows via Spark and verify with DuckDB JDBC") {
    // Create Spark session with DuckLake catalog
    val spark = SparkSession.builder()
      .appName("DuckLake Integration Test")
      .master("local[2]")
      .config("spark.sql.catalog.ducklake", "com.motherduck.spark.catalog.DuckLakeCatalog")
      .config("spark.sql.catalog.ducklake.path", s"ducklake:$catalogPath")
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("WARN")

      // Create test data
      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false)
      ))

      val data = Seq(
        Row(1, "Alice", 30),
        Row(2, "Bob", 25),
        Row(3, "Charlie", 35)
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )

      // Write using DuckLake connector
      df.writeTo("ducklake.main.users").append()

    } finally {
      // Explicitly close the DuckLake catalog to release the file lock
      val catalog = spark.sessionState.catalogManager.catalog("ducklake")
      catalog match {
        case c: AutoCloseable => c.close()
        case _ => // Not closeable
      }
      // Stop Spark
      spark.stop()
    }

    // Now verify the data was written
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try {
      val stmt = conn.createStatement()
      try {
        stmt.execute("INSTALL ducklake")
        stmt.execute("LOAD ducklake")
        stmt.execute(s"ATTACH 'ducklake:$catalogPath' AS verify_lake")

        val rs = stmt.executeQuery(
          "SELECT id, name, age FROM verify_lake.main.users ORDER BY id"
        )

        val results = scala.collection.mutable.ArrayBuffer[(Int, String, Int)]()
        while (rs.next()) {
          results += ((rs.getInt("id"), rs.getString("name"), rs.getInt("age")))
        }
        rs.close()

        assert(results.length == 3, s"Expected 3 rows, got ${results.length}")
        assert(results(0) == (1, "Alice", 30))
        assert(results(1) == (2, "Bob", 25))
        assert(results(2) == (3, "Charlie", 35))

      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }

    // Also verify data files exist
    val dataDir = tempDir.resolve("data").resolve("main").resolve("users")
    assert(Files.exists(dataDir), s"Data directory should exist: $dataDir")

    val parquetFiles = Files.list(dataDir)
      .filter(p => p.toString.endsWith(".parquet") || Files.isDirectory(p))
      .count()

    assert(parquetFiles > 0, "Should have written at least one Parquet file or job directory")
  }
}
