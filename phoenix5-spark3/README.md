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

phoenix-spark extends Phoenix's MapReduce support to allow Spark to load Phoenix tables as DataFrames,
and enables persisting DataFrames back to Phoenix.

## Configuring Spark to use the connector

Use the shaded connector JAR `phoenix5-spark3-shaded-6.0.0-SNAPSHOT.jar` .
Apart from the shaded connector JAR, you also need to add the hbase mapredcp libraries and the hbase configuration directory to the classpath. The final classpath should be something like

`/etc/hbase/conf:$(hbase mapredcp):phoenix5-spark3-shaded-6.0.0-SNAPSHOT.jar`

(add the exact paths as appropiate to your system)
Both the `spark.driver.extraClassPath` and `spark.executor.extraClassPath` properties need to be set the above classpath. You may add them spark-defaults.conf, or specify them on the spark-shell or spark-submit command line.

## Configuration properties

| Name                      | Default | Usage | Description |
|---------------------------|---------|-------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| table                     | empty   |  R/W  | table name as `namespace.table_name` |
| zkUrl                     | empty   |  R/W  | (Optional) List of zookeeper hosts. Deprecated, use `jdbcUrl` instead. Recommended not to set, value will be taken from hbase-site.xml |
| jdbcUrl                   | empty   |  R/W  | (Optional) jdbc url connection to database as `jdbc:phoenix:zkHost:zkport`. Recommended not to set, value will be taken from hbase-site.xml |
| dateAsTimestamp           | false   |  R    | Cast Date to Timestamp |
| doNotMapColumnFamily      | false   |  R    | For non default column family. Do not prefix column with column family name |
| TenantId                  | empty   |  R/W  | Define tenantId when reading from multitenant table |
| phoenixconfigs            | empty   |  R/W  | Comma seperated value of hbase/phoenix config to override. (property=value,property=value) |
| skipNormalizingIdentifier | empty   |  W    | skip normalize identifier |

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
  .config("spark.hadoopRDD.ignoreEmptySplits", "false")
  .getOrCreate()

// Load data from TABLE1
val df = spark.sqlContext
  .read
  .format("phoenix")
  .option("table", "TABLE1")
  .load

df.filter(df("COL1") === "test_row_1" && df("ID") === 1L)
  .select(df("ID"))
  .show

spark.stop()
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
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("phoenix-test")
            .set("spark.hadoopRDD.ignoreEmptySplits", "false");
        SparkSessinon spark = SparkSession.builder().config(sparkConf).getOrCreate();
        
        // Load data from TABLE1
        Dataset<Row> df = spark
            .read()
            .format("phoenix")
            .option("table", "TABLE1")
            .load();
        df.createOrReplaceTempView("TABLE1");
    
        df = spark.sql("SELECT * FROM TABLE1 WHERE COL1='test_row_1' AND ID=1L");
        df.show();

      spark.stop();
    }
}
```

### Load as a DataFrame using SparkSql and the DataSourceV2 API
Scala example:
```scala
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

val spark = SparkSession
  .builder()
  .appName("phoenix-test")
  .master("local")
  .config("spark.hadoopRDD.ignoreEmptySplits", "false")
  .getOrCreate()

// Load data from TABLE1
spark.sql("CREATE TABLE TABLE1_SQL USING phoenix OPTIONS ('table' 'TABLE1')")

val df = spark.sql(s"SELECT ID FROM TABLE1_SQL where COL1='test_row_1'")

df.show

spark.stop()
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
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("phoenix-test")
            .set("spark.hadoopRDD.ignoreEmptySplits", "false");
        SparkSessinon spark = SparkSession.builder().config(sparkConf).getOrCreate();
        
        // Load data from TABLE1
        Dataset<Row> df = spark.sql("CREATE TABLE TABLE1_SQL USING phoenix OPTIONS ('table' 'TABLE1'");    
        
        df = spark.sql("SELECT * FROM TABLE1_SQL WHERE COL1='test_row_1' AND ID=1L");
        df.show();

        spark.stop();
    }
}
```

## Saving to Phoenix

### Save DataFrames to Phoenix using DataSourceV2

The `save` is method on DataFrame allows passing in a data source type. You can use
`phoenix` for DataSourceV2 and must also pass in a `table` parameter to
specify which table to persist the DataFrame to. The column names are derived from
the DataFrame's schema field names, and must match the Phoenix column names.

The `save` method also takes a `SaveMode` option, for which only `SaveMode.Append` is supported.

Given two Phoenix tables with the following DDL:

```sql
CREATE TABLE INPUT_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);
CREATE TABLE OUTPUT_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);
```
you can load from an input table and save to an output table as a DataFrame as follows in Scala:

```scala
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession, SaveMode}

val spark = SparkSession
  .builder()
  .appName("phoenix-test")
  .master("local")
  .config("spark.hadoopRDD.ignoreEmptySplits", "false")
  .getOrCreate()
  
// Load INPUT_TABLE
val df = spark.sqlContext
  .read
  .format("phoenix")
  .option("table", "INPUT_TABLE")
  .load

// Save to OUTPUT_TABLE
df.write
  .format("phoenix")
  .mode(SaveMode.Append)
  .option("table", "OUTPUT_TABLE")
  .save()
spark.stop()
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
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("phoenix-test")
            .set("spark.hadoopRDD.ignoreEmptySplits", "false");
        SparkSessinon spark = SparkSession.builder().config(sparkConf).getOrCreate();
        
        // Load INPUT_TABLE
        Dataset<Row> df = spark
            .read()
            .format("phoenix")
            .option("table", "INPUT_TABLE")
            .load();
        
        // Save to OUTPUT_TABLE
        df.write()
          .format("phoenix")
          .mode(SaveMode.Append)
          .option("table", "OUTPUT_TABLE")
          .save();

        spark.stop();
    }
}
```

### Save from an external RDD with a schema to a Phoenix table

Just like the previous example, you can pass in the data source type as `phoenix` and specify the `table`  parameter
indicating which table to persist the DataFrame to.

Note that the schema of the RDD must match its column data and this must match the schema of the Phoenix table
that you save to.

Given an output Phoenix table with the following DDL:

```sql
CREATE TABLE OUTPUT_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);
```
you can save a dataframe from an RDD as follows in Scala: 

```scala
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType, StructField}
import org.apache.spark.sql.{Row, SQLContext, SparkSession, SaveMode}

val spark = SparkSession
  .builder()
  .appName("phoenix-test")
  .master("local")
  .config("spark.hadoopRDD.ignoreEmptySplits", "false")
  .getOrCreate()
  
val dataSet = List(Row(1L, "1", 1), Row(2L, "2", 2), Row(3L, "3", 3))

val schema = StructType(
  Seq(StructField("ID", LongType, nullable = false),
    StructField("COL1", StringType),
    StructField("COL2", IntegerType)))

val rowRDD = spark.sparkContext.parallelize(dataSet)

// Apply the schema to the RDD.
val df = spark.createDataFrame(rowRDD, schema)

df.write
  .format("phoenix")
  .option("table", "OUTPUT_TABLE")
  .mode(SaveMode.Append)
  .save()

spark.stop()
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
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("phoenix-test")
            .set("spark.hadoopRDD.ignoreEmptySplits", "false");
        SparkSessinon spark = SparkSession.builder().config(sparkConf).getOrCreate();
  
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
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.write()
            .format("phoenix")
            .mode(SaveMode.Append)
            .option("table", "OUTPUT_TABLE")
            .save();
        
        spark.stop();
    }
}
```

## Notes

- The DataSourceV2 based "phoenix" data source accepts the `"jdbcUrl"` parameter, which can be
used to override the default Hbase/Phoenix instance specified in hbase-site.xml. It also accepts
the deprected `zkUrl` parameter for backwards compatibility purposes. If neither is specified,
it falls back to using connection defined by hbase-site.xml.
- `"jdbcUrl"` expects a full Phoenix JDBC URL, i.e. "jdbc:phoenix" or "jdbc:phoenix:zkHost:zkport",
while `"zkUrl"` expects the ZK quorum only, i.e. "zkHost:zkPort"
- If you want to use DataSourceV1, you can use source type `"org.apache.phoenix.spark"`
  instead of `"phoenix"`, however this is deprecated.
  The `"org.apache.phoenix.spark"` datasource does not accept the `"jdbcUrl"` parameter,
  only `"zkUrl"`
- The (deprecated) functions `phoenixTableAsDataFrame`, `phoenixTableAsRDD` and
  `saveToPhoenix` use the deprecated `"org.apache.phoenix.spark"` datasource, and allow
  optionally specifying a `conf` Hadoop configuration parameter with custom Phoenix client settings,
  as well as an optional `zkUrl` parameter.

- As of [PHOENIX-5197](https://issues.apache.org/jira/browse/PHOENIX-5197), you can pass configurations from the driver
to executors as a comma-separated list against the key `phoenixConfigs` i.e (PhoenixDataSource.PHOENIX_CONFIGS), for ex:
    ```scala
    df = spark
      .sqlContext
      .read
      .format("phoenix")
      .options(Map("table" -> "Table1", "phoenixConfigs" -> "hbase.client.retries.number=10,hbase.client.pause=10000"))
      .load;
    ```
    This list of properties is parsed and populated into a properties map which is passed to `DriverManager.getConnection(connString, propsMap)`.
    Note that the same property values will be used for both the driver and all executors and
    these configurations are used each time a connection is made (both on the driver and executors).

- As of [PHOENIX-7377](https://issues.apache.org/jira/browse/PHOENIX-7377), you can pass boolean parameter to avoid mapping
  non default family columns to `columnFamily.columnName` by setting the key `doNotMapColumnFamily` to `true` (default value: `false`), for ex:
    ```scala
    df = spark
      .sqlContext
      .read
      .format("phoenix")
      .options(Map("table" -> "Table1", "doNotMapColumnFamily" -> "true"))
      .load;
    ```
  
## Limitations

- Basic support for column and predicate pushdown using the Data Source API
- The Data Source API does not support passing custom Phoenix settings in configuration, you must
create the DataFrame or RDD directly if you need fine-grained configuration.
- No support for aggregate or distinct functions (http://phoenix.apache.org/phoenix_mr.html)

## Deprecated Usages

### Load as a DataFrame directly using a Configuration object
```scala
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._

val configuration = new Configuration()
// Can set Phoenix-specific settings, requires 'hbase.zookeeper.quorum'

val sparkConf = new SparkConf()
val sc = new SparkContext("local", "phoenix-test", sparkConf)
val sqlContext = new SQLContext(sc)

// Load the columns 'ID' and 'COL1' from TABLE1 as a DataFrame
val df = sqlContext.phoenixTableAsDataFrame(
  "TABLE1", Array("ID", "COL1"), conf = configuration
)

df.show
```

### Load as an RDD, using a Zookeeper URL
```scala
import org.apache.spark.SparkConf
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
correspond to the Java types Phoenix supports (http://phoenix.apache.org/language/datatypes.html)

Given a Phoenix table with the following DDL:

```sql
CREATE TABLE OUTPUT_TEST_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);
```

```scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.phoenix.spark._

val sparkConf = new SparkConf()
val sc = new SparkContext("local", "phoenix-test", sparkConf)
val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))

sc
  .parallelize(dataSet)
  .saveToPhoenix(
    "OUTPUT_TEST_TABLE",
    Seq("ID","COL1","COL2"),
    zkUrl = Some("phoenix-server:2181")
  )
```
