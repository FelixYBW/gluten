import os

scaleFactor = os.environ["ENV_SCALEFACTOR"]
numPartitions = os.environ["ENV_PARALLELISM"]
dataformat = "parquet" # data format of data source
dataSourceCodec = os.environ["ENV_CODEC"]
rootDir = os.environ["ENV_ROOTDIR"]
tpchDir = rootDir + f"/tpch_sf{scaleFactor}_{dataformat}_{dataSourceCodec}" # root directory of location to create data in.

print(f'scaleFactor = {scaleFactor}')
print(f'numPartitions = {numPartitions}')
print(f'dataformat = {dataformat}')
print(f'rootDir = {rootDir}')

scala=f'''import com.databricks.spark.sql.perf.tpch._


val scaleFactor = "{scaleFactor}" // scaleFactor defines the size of the dataset to generate (in GB).
val numPartitions = {numPartitions}  // how many dsdgen partitions to run - number of input tasks.

val format = "{dataformat}" // valid spark format like parquet "parquet".
val rootDir = "{tpchDir}" // root directory of location to create data in.
val dbgenDir = "/opt/datagen/tpch-dbgen" // location of dbgen

val tables = new TPCHTables(spark.sqlContext,
    dbgenDir = dbgenDir,
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
    useStringForDate = false) // true to replace DateType with StringType


tables.genData(
    location = rootDir,
    format = format,
    overwrite = true, // overwrite the data that is already there
    partitionTables = false, // do not create the partitioned fact tables
    clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files.
    filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
    tableFilter = "", // "" means generate all tables
    numPartitions = numPartitions) // how many dsdgen partitions to run - number of input tasks.
'''

with open("/opt/datagen/tpch_datagen_parquet.scala","w") as f:
    f.writelines(scala)

# Verify parameters
print(f'--conf spark.sql.shuffle.partitions={numPartitions}')
print(f'--conf spark.sql.parquet.compression.codec={dataSourceCodec}')

tpch_datagen_parquet=f'''
cat /opt/datagen/tpch_datagen_parquet.scala | {os.environ['SPARK_HOME']}/bin/spark-shell \
  --name tpch_gen_parquet \
  --driver-memory 10g \
  --conf spark.sql.shuffle.partitions={numPartitions} \
  --conf spark.sql.parquet.compression.codec={dataSourceCodec} \
  --conf spark.network.timeout=800s \
  --conf spark.executor.heartbeatInterval=200s \
  --jars /opt/datagen/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
'''

with open("/opt/datagen/tpch_datagen_parquet.sh","w") as f:
    f.writelines(tpch_datagen_parquet)

os.system("bash /opt/datagen/tpch_datagen_parquet.sh")

exit(0)

# to generate table
import findspark
findspark.init()

import os
import time
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = (SparkConf()
    .set('spark.app.name', 'generate_schema')
       )

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SQLContext(sc)
time.sleep(10)

spark.sql(f"create database tpch_sf{scaleFactor}_{dataformat}_{dataSourceCodec} location {rootDir}/tpch_sf{scaleFactor}_{dataformat}_{dataSourceCodec}'")
spark.sql("use tpch_sf{scaleFactor}_{dataformat}_{dataSourceCodec}")
for t in ['customer',
          'lineitem',
          'nation',
          'orders',
          'part',
          'partsupp',
          'region',
          'supplier']:
    spark.sql(f"create table table_{t} using parquet location '{rootDir}/tpch_sf{scaleFactor}_{dataformat}_{dataSourceCodec}/{t}' ")
    df2=spark.sql("select * from table_"+t)
    spark.sql(f"create view {t} as select " + ",".join([c+' as ' + c.split("_")[1] for c in df2.columns]) + " from table_"+t)