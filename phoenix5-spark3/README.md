<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Phoenix5-Spark3 Connector

The phoenix5-spark3 plugin extends Phoenix's MapReduce support to allow Spark
 to load Phoenix tables as DataFrames,
 and enables persisting DataFrames back to Phoenix.

## Pre-Requisites

* Phoenix 5.1.2+
* Spark 3.0.3+

## Why not JDBC?

Although Spark supports connecting directly to JDBC databases,
 It’s only able to parallelize queries by partioning on a numeric column.
 It also requires a known lower bound,
 upper bound and partition count in order to create split queries.

In contrast, the phoenix-spark integration is able to leverage the underlying splits provided by Phoenix in order to retrieve and save data across multiple workers. All that’s required is a database URL and a table name. Optional SELECT columns can be given, as well as pushdown predicates for efficient filtering.

The choice of which method to use to access Phoenix comes down to each specific use case.

## Setup

To setup connector add `phoenix5-spark3-shaded` JAR as a dependency in your Spark job like -

```xml
<dependency>
  <groupId>org.apache.phoenix</groupId>
  <artifactId>phoenix5-spark3-shaded</artifactId>
  <version>${phoenix.connectors.version}</version>
</dependency>
```

Additionally, You must add the hbase mapredcp libraries and the hbase configuration directory to the classpath. The final classpath should be something like -

`/etc/hbase/conf:$(hbase mapredcp):phoenix5-spark3-shaded-{phoenix.connectors.version}.jar`

NOTE:

* Use the exact paths as appropiate to your system.
* Set both `spark.driver.extraClassPath` and `spark.executor.extraClassPath` properties to the aforementioned classpath. You can add them to the `spark-defaults.conf`, Or specify them in the `spark-shell` or `spark-submit` command line utilities.

## Reading Phoenix Tables

Given a Phoenix table with the following DDL and DML:

```sql
CREATE TABLE TABLE1 (ID BIGINT NOT NULL PRIMARY KEY, COL1 VARCHAR);
UPSERT INTO TABLE1 (ID, COL1) VALUES (1, 'test_row_1');
UPSERT INTO TABLE1 (ID, COL1) VALUES (2, 'test_row_2');
```

### Load as a DataFrame using the DataSourceV2 API

Scala example:

```scala
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

val spark = SparkSession
  .builder()
  .appName("phoenix-test")
  .master("local")
  .getOrCreate()

// Load data from TABLE1
val df = spark.sqlContext
  .read
  .format("phoenix")
  .options(Map("table" -> "TABLE1", "zkUrl" -> "phoenix-server:2181"))
  .load

df.filter(df("COL1") === "test_row_1" && df("ID") === 1L)
  .select(df("ID"))
  .show
```

Java example:

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class PhoenixSparkRead {
    
    public static void main() throws Exception {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("phoenix-test");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);
        
        // Load data from TABLE1
        Dataset<Row> df = sqlContext
            .read()
            .format("phoenix")
            .option("table", "TABLE1")
            .option("zkUrl", "phoenix-server:2181")
            .load();
        df.createOrReplaceTempView("TABLE1");
    
        SQLContext sqlCtx = new SQLContext(jsc);
        df = sqlCtx.sql("SELECT * FROM TABLE1 WHERE COL1='test_row_1' AND ID=1L");
        df.show();
        jsc.stop();
    }
}
```

PySpark example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

ss = SparkSession.builder.appName("phoenix-test").getOrCreate()
df = ss.read.format("phoenix").option("table", "TABLE1").option("zkUrl", "phoenix-server:2181").load()

# Approach - 1
df.filter((df.COL1 == "test_row_1") | (df.ID == 1)).select(col("ID")).show()

# Approach - 2
df.createOrReplaceTempView("TABLE1_TEMP")
ss.sql("SELECT * FROM TABLE1_TEMP WHERE COL1='test_row_1' AND ID=1L").show()
```

## Saving to Phoenix

### Save DataFrames to Phoenix using DataSourceV2

The `save` is method on DataFrame allows passing in a data source type. You can use
`phoenix` for DataSourceV2 and must also pass in a `table` and `zkUrl` parameter to
specify which table and server to persist the DataFrame to. The column names are derived from
the DataFrame's schema field names, and must match the Phoenix column names.

The `save` method also takes a `SaveMode` option, for which only `SaveMode.Append` is supported.

Given two Phoenix tables with the following DDL:

```sql
CREATE TABLE INPUT_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);
CREATE TABLE OUTPUT_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);

UPSERT INTO INPUT_TABLE (ID, COL1, COL2) VALUES (1, 'test_row_1', 1);
UPSERT INTO INPUT_TABLE (ID, COL1, COL2) VALUES (2, 'test_row_2', 2);
```

You can load from an input table and save to an output table as a DataFrame as follows:

Scala example:

```scala
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession, SaveMode}

val spark = SparkSession
  .builder()
  .appName("phoenix-test")
  .master("local")
  .getOrCreate()
  
// Load INPUT_TABLE
val df = spark.sqlContext
  .read
  .format("phoenix")
  .options(Map("table" -> "INPUT_TABLE", "zkUrl" -> "phoenix-server:2181"))
  .load

// Save to OUTPUT_TABLE
df.write
  .format("phoenix")
  .mode(SaveMode.Append)
  .options(Map("table" -> "OUTPUT_TABLE", "zkUrl" -> "phoenix-server:2181"))
  .save()
```

Java example:

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SQLContext;

public class PhoenixSparkWriteFromInputTable {
    
    public static void main() throws Exception {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("phoenix-test");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);
        
        // Load INPUT_TABLE
        Dataset<Row> df = sqlContext
            .read()
            .format("phoenix")
            .option("table", "INPUT_TABLE")
            .option("zkUrl", "phoenix-server:2181")
            .load();
        
        // Save to OUTPUT_TABLE
        df.write()
          .format("phoenix")
          .mode(SaveMode.Append)
          .option("table", "OUTPUT_TABLE")
          .option("zkUrl", "phoenix-server:2181")
          .save();
        jsc.stop();
    }
}
```

PySpark example:

```python
from pyspark.sql import SparkSession

ss = SparkSession.builder.appName("phoenix-test").getOrCreate()

df = ss.read.format("phoenix").option("table", "INPUT_TABLE").option("zkUrl", "phoenix-server:2181").load()
df.write.format("phoenix").option("table", "OUTPUT_TABLE").option("zkUrl", "phoenix-server:2181").mode("append").save()
```

### Save from an external RDD with a schema to a Phoenix table

Just like the previous example, you can pass in the data source type as `phoenix` and specify the `table` and
`zkUrl` parameters indicating which table and server to persist the DataFrame to.

Note that the schema of the RDD must match its column data and this must match the schema of the Phoenix table
that you save to.

Given an output Phoenix table with the following DDL:

```sql
CREATE TABLE OUTPUT_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);
```

You can save a dataframe from an RDD as follows:

Scala example:

```scala
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType, StructField}
import org.apache.spark.sql.{Row, SQLContext, SparkSession, SaveMode}

val spark = SparkSession
  .builder()
  .appName("phoenix-test")
  .master("local")
  .getOrCreate()
  
val dataSet = List(Row(1L, "1", 1), Row(2L, "2", 2), Row(3L, "3", 3))

val schema = StructType(
  Seq(StructField("ID", LongType, nullable = false),
    StructField("COL1", StringType),
    StructField("COL2", IntegerType)))

val rowRDD = spark.sparkContext.parallelize(dataSet)

// Apply the schema to the RDD.
val df = spark.sqlContext.createDataFrame(rowRDD, schema)

df.write
  .format("phoenix")
  .options(Map("table" -> "OUTPUT_TABLE", "zkUrl" -> "phoenix-server:2181"))
  .mode(SaveMode.Append)
  .save()
```

Java example:

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class PhoenixSparkWriteFromRDDWithSchema {
 
    public static void main() throws Exception {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("phoenix-test");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);
        SparkSession spark = sqlContext.sparkSession();
        Dataset<Row> df;
  
        // Generate the schema based on the fields
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("ID", DataTypes.LongType, false));
        fields.add(DataTypes.createStructField("COL1", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("COL2", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(fields);
  
        // Generate the rows with the same exact schema
        List<Row> rows = new ArrayList<>();
        for (int i = 1; i < 4; i++) {
            rows.add(RowFactory.create(Long.valueOf(i), String.valueOf(i), i));
        }
  
        // Create a DataFrame from the rows and the specified schema
        df = spark.createDataFrame(rows, schema);
        df.write()
            .format("phoenix")
            .mode(SaveMode.Append)
            .option("table", "OUTPUT_TABLE")
            .option("zkUrl",  "phoenix-server:2181")
            .save();
  
        jsc.stop();
    }
}
```

PySpark example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType

ss = SparkSession.builder.appName("phoenix-test").getOrCreate()
schema = StructType([StructField("ID", LongType()), StructField("COL1", StringType()), StructField("COL2", LongType())])
dataSet = [Row(1, "1", 1),Row(2, "2", 2), Row(3, "3", 3)]

rdd = ss.sparkContext.parallelize(data)
df = ss.createDataFrame(rdd, schema)

df.write.format("phoenix").option("table", "OUTPUT_TABLE").option("zkUrl", "phoenix-server:2181").mode("append").save()
```

## Notes

* If you want to use DataSourceV1, you can use source type `"org.apache.phoenix.spark"` 
  instead of `"phoenix"`, however this is deprecated as of `connectors-1.0.0`.
* The (deprecated) functions `phoenixTableAsDataFrame`, `phoenixTableAsRDD` and `saveToPhoenix` all support
optionally specifying a `conf` Hadoop configuration parameter with custom Phoenix client settings,
as well as an optional `zkUrl` parameter for the Phoenix connection URL.
* If `zkUrl` isn't specified, it's assumed that the "hbase.zookeeper.quorum" property has been set
in the `conf` parameter. Similarly, if no configuration is passed in, `zkUrl` must be specified.
* As of [PHOENIX-5197]("https://issues.apache.org/jira/browse/PHOENIX-5197"), you can pass configurations from the driver
to executors as a comma-separated list against the key `phoenixConfigs` i.e (PhoenixDataSource.PHOENIX_CONFIGS), for ex:

    ```scala
    df = spark
      .sqlContext
      .read
      .format("phoenix")
      .options(Map("table" -> "Table1", "zkUrl" -> "phoenix-server:2181", "phoenixConfigs" -> "hbase.client.retries.number=10,hbase.client.pause=10000"))
      .load;
    ```

    This list of properties is parsed and populated into a properties map which is passed to `DriverManager.getConnection(connString, propsMap)`.
    Note that the same property values will be used for both the driver and all executors and
    these configurations are used each time a connection is made (both on the driver and executors).

## Limitations

* Basic support for column and predicate pushdown using the Data Source API
* The Data Source API does not support passing custom Phoenix settings in configuration, you must
create the DataFrame or RDD directly if you need fine-grained configuration.
* No support for aggregate or distinct functions (http://phoenix.apache.org/phoenix_mr.html)

## Limitations of the Spark3 connector comapred to the Spark2 Connector

* Non-uppercase column names cannot be used for mapping DataFrames. (PHOENIX-6668)
* When writing to a DataFrame, every SQL column in the table must be specified. (PHOENIX-6667)

## Deprecated Usages

### Load as a DataFrame directly using a Configuration object

```scala
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._

val configuration = new Configuration()
// Can set Phoenix-specific settings, requires 'hbase.zookeeper.quorum'

val sc = new SparkContext("local", "phoenix-test")
val sqlContext = new SQLContext(sc)

// Load the columns 'ID' and 'COL1' from TABLE1 as a DataFrame
val df = sqlContext.phoenixTableAsDataFrame(
  "TABLE1", Array("ID", "COL1"), conf = configuration
)

df.show
```

### Load as an RDD, using a Zookeeper URL

```scala
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD

val sc = new SparkContext("local", "phoenix-test")

// Load the columns 'ID' and 'COL1' from TABLE1 as an RDD
val rdd: RDD[Map[String, AnyRef]] = sc.phoenixTableAsRDD(
  "TABLE1", Seq("ID", "COL1"), zkUrl = Some("phoenix-server:2181")
)

rdd.count()

val firstId = rdd.first()("ID").asInstanceOf[Long]
val firstCol = rdd.first()("COL1").asInstanceOf[String]
```

### Saving RDDs to Phoenix

`saveToPhoenix` is an implicit method on RDD[Product], or an RDD of Tuples. The data types must
correspond to the Java types Phoenix supports ("http://phoenix.apache.org/language/datatypes.html")

Given a Phoenix table with the following DDL:

```sql
CREATE TABLE OUTPUT_TEST_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);
```

```scala
import org.apache.spark.SparkContext
import org.apache.phoenix.spark._

val sc = new SparkContext("local", "phoenix-test")
val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))

sc
  .parallelize(dataSet)
  .saveToPhoenix(
    "OUTPUT_TEST_TABLE",
    Seq("ID","COL1","COL2"),
    zkUrl = Some("phoenix-server:2181")
  )
```
