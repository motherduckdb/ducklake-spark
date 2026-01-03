# DuckLake Spark Connector

A Spark DataSource V2 connector for writing to [DuckLake](https://duckdb.org/docs/extensions/ducklake).
Note that the ducklake and the tables must already exist before writing.

## Installation

Maven central TBD. For now, get the jar from artifacts (or build yourself) and use it directly.

```bash
spark-submit --jars ducklake-spark_2.12-0.1.0-SNAPSHOT.jar your_script.py
```

## Connecting

This connector uses Spark/hadoop libraries to write Parquet to S3 (or another blob storage, in principle). 
Therefore, your Spark executor nodes will need write access to the blob storage which can be done by passing them through the spark session configuration or any other way [Hadoop can authenticate](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authenticating_with_S3).

```
spark = SparkSession.builder \
      .config("spark.hadoop.fs.s3a.access.key", "AKIA...") \
      .config("spark.hadoop.fs.s3a.secret.key", "...") \
      ... 
      .getOrCreate()
```

The connector also uses DuckDB ducklake extension to interact with the DuckLake catalog, so unless the catalog is a local DuckDB database, it will need to pass the credentials.

### MotherDuck

Pass `motherduck_token` through spark session configuration:

```
spark = SparkSession.builder \
    .config("spark.sql.catalog.ducklake.motherduck-token", "...")
    ... 
    .getOrCreate()
```

Environment variable `motherduck_token` will also work.

### Postgres

```
spark = SparkSession.builder \
    .config("spark.sql.catalog.ducklake.path", "ducklake:postgres:") \
    .config("spark.sql.catalog.ducklake.init-sql", """ 
        create secret (
            type postgres, 
            host '...', 
            port 5432, 
            database THE_DATABASE_NAME, 
            user 'THE_POSTGRES_USER',
            password 'THE_PASSWORD');
    """) \
    ...
    .getOrCreate()
```

## Examples

### MotherDuck bring-your-own-bucket

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("My DuckLake Test") \
    .master("spark://...:7077") \
    .config("spark.jars", "/home/elena/code/ecosystems2/ducklake_spark/repos/ducklake-spark/target/ducklake-spark_2.12-0.1.0-SNAPSHOT.jar") \
    .config("spark.sql.catalog.ducklake", "com.motherduck.spark.catalog.DuckLakeCatalog") \
    .config("spark.sql.catalog.ducklake.path", "md:THE_DUCKLAKE_DATABASE_NAME") \
    .config("spark.sql.catalog.ducklake.motherduck-token", "THE_MOTHERDUCK_TOKEN") \
    .config("spark.jars.packages",
              "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.access.key", THE_AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", THE_AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.region", THE_AWS_REGION) \
    .getOrCreate()


# Create a DataFrame
df = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
], ["id", "name"])

# Write to DuckLake table (the table needs to already have been created)
df.writeTo("ducklake.main.users").append()

spark.stop()
```

### Postgres

```
spark = SparkSession.builder \
    .appName("...") \
    .master("...") \
    .config("spark.jars", "/location/to/jar/ducklake-spark_2.12-0.1.0-SNAPSHOT.jar") \
    .config("spark.sql.catalog.ducklake", "com.motherduck.spark.catalog.DuckLakeCatalog") \
    .config("spark.sql.catalog.ducklake.path", "ducklake:postgres:") \
    .config("spark.sql.catalog.ducklake.init-sql", """ 
        create secret (
            type postgres, 
            host '...', 
            port 5432, 
            database THE_DATABASE_NAME, 
            user 'THE_POSTGRES_USER',
            password 'THE_PASSWORD');
    """) \
    .config("spark.hadoop.fs.s3a.access.key", '...') \
    .config("spark.hadoop.fs.s3a.secret.key", '...') \
    .config("spark.hadoop.fs.s3a.region", '...') \    
    .getOrCreate()
```

### Local DuckDB catalog

```
spark = SparkSession.builder \
    .appName("My Ducklake Test") \
    .master("spark://...:7077") \
    .config("spark.jars", "/home/elena/code/ecosystems2/ducklake_spark/repos/ducklake-spark/target/ducklake-spark_2.12-0.1.0-SNAPSHOT.jar") \
    .config("spark.sql.catalog.ducklake", "com.motherduck.spark.catalog.DuckLakeCatalog") \
    .config("spark.sql.catalog.ducklake.path", "ducklake:/path/to/ducklake/database_name.ducklake") \
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


## Troubleshooting

### Problems writing to S3

If access to S3 is denied, check that the credentials _and the region_ are correct.

```python
spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.region", "us-east-2") \
    ...
    .getOrCreate()
```

If hadoop/s3 jars cannot be found, either place them into $SPARK_HOME/jars or provide them through the spark session configuration
```
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    ...
    .getOrCreate()
```

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

# Or build the project without integration tests
mvn clean package -DskipTests
```


Connector fat jar will be in `target/ducklake-spark_2.12-0.1.0-SNAPSHOT.jar`.
