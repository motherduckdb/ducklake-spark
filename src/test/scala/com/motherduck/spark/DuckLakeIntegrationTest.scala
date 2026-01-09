package com.motherduck.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}
import java.sql.{Date, DriverManager, Timestamp}
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

    // Create DuckLake catalog and tables using pure DuckDB JDBC
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

        // Create table with supported types
        // Note: UUID and BLOB excluded - Spark writes them as STRING/BINARY which
        // causes type mismatch when DuckLake registers the Parquet files
        stmt.execute(
          """CREATE TABLE test_lake.main.all_types (
            |  col_boolean BOOLEAN,
            |  col_tinyint TINYINT,
            |  col_smallint SMALLINT,
            |  col_integer INTEGER,
            |  col_bigint BIGINT,
            |  col_float FLOAT,
            |  col_double DOUBLE,
            |  col_decimal DECIMAL(10, 2),
            |  col_varchar VARCHAR,
            |  col_date DATE,
            |  col_timestamp TIMESTAMP
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

  test("write all supported types via Spark and verify with DuckDB JDBC") {
    val spark = SparkSession.builder()
      .appName("DuckLake All Types Test")
      .master("local[2]")
      .config("spark.sql.catalog.ducklake", "com.motherduck.spark.catalog.DuckLakeCatalog")
      .config("spark.sql.catalog.ducklake.path", s"ducklake:$catalogPath")
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("WARN")

      // Schema matching all_types table
      val schema = StructType(Seq(
        StructField("col_boolean", BooleanType, nullable = true),
        StructField("col_tinyint", ByteType, nullable = true),
        StructField("col_smallint", ShortType, nullable = true),
        StructField("col_integer", IntegerType, nullable = true),
        StructField("col_bigint", LongType, nullable = true),
        StructField("col_float", FloatType, nullable = true),
        StructField("col_double", DoubleType, nullable = true),
        StructField("col_decimal", DecimalType(10, 2), nullable = true),
        StructField("col_varchar", StringType, nullable = true),
        StructField("col_date", DateType, nullable = true),
        StructField("col_timestamp", TimestampType, nullable = true)
      ))

      // Test data with representative values
      val testDate = Date.valueOf("2024-06-15")
      val testTimestamp = Timestamp.valueOf("2024-06-15 10:30:45")

      val data = Seq(
        Row(
          true,                                    // col_boolean
          42.toByte,                               // col_tinyint
          1000.toShort,                            // col_smallint
          123456,                                  // col_integer
          9876543210L,                             // col_bigint
          3.14f,                                   // col_float
          2.718281828,                             // col_double
          new java.math.BigDecimal("12345.67"),   // col_decimal
          "hello world",                           // col_varchar
          testDate,                                // col_date
          testTimestamp                            // col_timestamp
        ),
        Row(
          false,                                   // col_boolean
          -128.toByte,                             // col_tinyint (min value)
          -32768.toShort,                          // col_smallint (min value)
          -2147483648,                             // col_integer (min value)
          -9223372036854775808L,                   // col_bigint (min value)
          -1.5f,                                   // col_float
          -999.999,                                // col_double
          new java.math.BigDecimal("-99999.99"),  // col_decimal
          "special chars: éàü 中文",               // col_varchar with unicode
          Date.valueOf("1970-01-01"),              // col_date (epoch)
          Timestamp.valueOf("1970-01-01 00:00:00") // col_timestamp (epoch)
        )
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )

      // Write using DuckLake connector
      df.writeTo("ducklake.main.all_types").append()

    } finally {
      val catalog = spark.sessionState.catalogManager.catalog("ducklake")
      catalog match {
        case c: AutoCloseable => c.close()
        case _ =>
      }
      spark.stop()
    }

    // Verify with DuckDB JDBC
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try {
      val stmt = conn.createStatement()
      try {
        stmt.execute("INSTALL ducklake")
        stmt.execute("LOAD ducklake")
        stmt.execute(s"ATTACH 'ducklake:$catalogPath' AS verify_lake")

        val rs = stmt.executeQuery("SELECT * FROM verify_lake.main.all_types ORDER BY col_integer")

        var rowCount = 0
        while (rs.next()) {
          rowCount += 1

          if (rowCount == 1) {
            // First row by col_integer order: -2147483648 (edge/negative values)
            assert(rs.getBoolean("col_boolean") == false)
            assert(rs.getByte("col_tinyint") == -128)
            assert(rs.getShort("col_smallint") == -32768)
            assert(rs.getInt("col_integer") == -2147483648)
            assert(rs.getLong("col_bigint") == -9223372036854775808L)
            assert(rs.getString("col_varchar") == "special chars: éàü 中文")
            assert(rs.getDate("col_date").toString == "1970-01-01")
          }

          if (rowCount == 2) {
            // Second row by col_integer order: 123456 (positive values)
            assert(rs.getBoolean("col_boolean") == true)
            assert(rs.getByte("col_tinyint") == 42)
            assert(rs.getShort("col_smallint") == 1000)
            assert(rs.getInt("col_integer") == 123456)
            assert(rs.getLong("col_bigint") == 9876543210L)
            assert(math.abs(rs.getFloat("col_float") - 3.14f) < 0.001f)
            assert(math.abs(rs.getDouble("col_double") - 2.718281828) < 0.0001)
            assert(rs.getBigDecimal("col_decimal").compareTo(new java.math.BigDecimal("12345.67")) == 0)
            assert(rs.getString("col_varchar") == "hello world")
            assert(rs.getDate("col_date").toString == "2024-06-15")
          }
        }
        rs.close()

        assert(rowCount == 2, s"Expected 2 rows, got $rowCount")

      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }
  }
}
