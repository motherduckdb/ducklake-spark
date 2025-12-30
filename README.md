# DuckLake Spark Connector

A Spark DataSource V2 connector for writing to [DuckLake](https://duckdb.org/docs/extensions/ducklake).


## Installation

### Maven Coordinates

```xml
<dependency>
    <groupId>com.motherduck</groupId>
    <artifactId>ducklake-spark_2.12</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Using the JAR directly

```bash
spark-submit --jars ducklake-spark_2.12-0.1.0-SNAPSHOT.jar your_script.py
```

## Quick Start

### PySpark Example

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DuckLake Example") \
    .config("spark.jars", "/path/to/ducklake-spark_2.12-0.1.0-SNAPSHOT.jar") \
    .config("spark.sql.catalog.ducklake", "com.motherduck.spark.catalog.DuckLakeCatalog") \
    .config("spark.sql.catalog.ducklake.connection", "ducklake:/path/to/catalog.ducklake") \
    .getOrCreate()

# Create a DataFrame
df = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
], ["id", "name"])

# Write to DuckLake table
df.writeTo("ducklake.main.users").append()

spark.stop()
```

### Connection Strings

The connector supports all DuckLake catalog backends:

| Backend | Connection String |
|---------|------------------|
| Local DuckDB | `ducklake:/path/to/catalog.ducklake` |
| MotherDuck Bring Your Own Bucket | `md:ducklake_database_name` |
| PostgreSQL | `ducklake:postgres://user:pass@host:5432/db` |
| MySQL | `ducklake:mysql://user:pass@host:3306/db` |

Note that the ducklake and the tables must already exist.

### Writing to S3

For S3-backed DuckLake tables, configure AWS credentials:

```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.ducklake", "com.motherduck.spark.catalog.DuckLakeCatalog") \
    .config("spark.sql.catalog.ducklake.connection", "md:my_ducklake_db") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.region", "us-east-2") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()
```


## Limitations

- **Write-only**: Read support is not yet implemented
- **Append mode only**: Overwrite and other modes not yet supported
- **No schema evolution**: Table schema must match DataFrame schema exactly
- **Type strictness**: Spark `LongType` won't work if table expects `INTEGER` - use explicit schemas


## See Also

- [DuckLake Documentation](https://duckdb.org/docs/extensions/ducklake)
- [MotherDuck DuckLake documentation](https://motherduck.com/docs/integrations/file-formats/ducklake/#bring-your-own-bucket)
- [ducklake-spark-mvp](../ducklake-spark-mvp/) - Alternative CommitProtocol-based approach


## Building the project

### Requirements

- JDK 17+
- Maven 3.8+
- Apache Spark 3.5.x for manual testing

### Clone and Build

```bash
git clone https://github.com/motherduck/ducklake-spark.git
cd ducklake-spark

# Build the project
mvn clean package
```

Connector fat jar will be in `target/ducklake-spark_2.12-0.1.0-SNAPSHOT.jar`.
