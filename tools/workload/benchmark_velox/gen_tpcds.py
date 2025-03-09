import os

scaleFactor = os.environ["ENV_SCALEFACTOR"]
numPartitions = os.environ["ENV_PARALLELISM"]
dataformat = "parquet" # data format of data source
dataSourceCodec = os.environ["ENV_CODEC"]
rootDir = os.environ["ENV_ROOTDIR"]
tpcdsDir = rootDir + f"/tpcds_sf{scaleFactor}_{dataformat}_{dataSourceCodec}" # root directory of location to create data in.

# Verify parameters
print(f'scaleFactor = {scaleFactor}')
print(f'numPartitions = {numPartitions}')
print(f'dataformat = {dataformat}')
print(f'rootDir = {rootDir}')

scala=f'''import com.databricks.spark.sql.perf.tpcds._

val scaleFactor = "{scaleFactor}" // scaleFactor defines the size of the dataset to generate (in GB).
val numPartitions = {numPartitions}  // how many dsdgen partitions to run - number of input tasks.

val format = "{dataformat}" // valid spark format like parquet "parquet".
val rootDir = "{tpcdsDir}" // root directory of location to create data in.
val dsdgenDir = "/opt/datagen/tpcds-kit/tools/" // location of dbgen

val tables = new TPCDSTables(spark.sqlContext,
    dsdgenDir = dsdgenDir,
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
    useStringForDate = false) // true to replace DateType with StringType


tables.genData(
    location = rootDir,
    format = format,
    overwrite = true, // overwrite the data that is already there
    partitionTables = true, // create the partitioned fact tables
    clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
    filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
    tableFilter = "", // "" means generate all tables
    numPartitions = numPartitions) // how many dsdgen partitions to run - number of input tasks.
'''

with open("/opt/datagen/tpcds_datagen_parquet.scala","w") as f:
    f.writelines(scala)

tpcds_datagen_parquet=f'''
cat /opt/datagen/tpcds_datagen_parquet.scala | {os.environ['SPARK_HOME']}/bin/spark-shell \
  --name tpcds_gen_parquet \
  --conf spark.sql.shuffle.partitions={numPartitions} \
  --conf spark.sql.parquet.compression.codec={dataSourceCodec} \
  --conf spark.network.timeout=800s \
  --conf spark.executor.heartbeatInterval=200s \
  --jars /opt/datagen/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
'''

with open("/opt/datagen/tpcds_datagen_parquet.sh","w") as f:
    f.writelines(tpcds_datagen_parquet)


os.system("bash /opt/datagen/tpcds_datagen_parquet.sh")

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

spark.sql(f"create database tpcds_sf{scaleFactor}_{dataformat}_{dataSourceCodec} location {rootDir}/tpcds_sf{scaleFactor}_{dataformat}_{dataSourceCodec}'")
spark.sql("use tpcds_sf{scaleFactor}_{dataformat}_{dataSourceCodec}")
for t in ['call_center',
          'catalog_page',
          'catalog_returns',
          'catalog_sales',
          'customer',
          'customer_address',
          'customer_demographics',
          'date_dim',
          'household_demographics',
          'income_band',
          'inventory',
          'item',
          'promotion',
          'reason',
          'ship_mode',
          'store',
          'store_returns',
          'store_sales',
          'time_dim',
          'warehouse',
          'web_page',
          'web_returns',
          'web_sales',
          'web_site']:
    spark.sql(f"create table {t} using parquet location '{rootDir}/tpcds_sf{scaleFactor}_{dataformat}_{dataSourceCodec}/{t}' ")