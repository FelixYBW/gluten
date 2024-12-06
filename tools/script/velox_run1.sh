
python ./monitor.py start

yarn application --list 2>&1 | grep application_ | awk '{print($1)}' | xargs yarn application --kill > /dev/null

/usr/lib/spark-3.2/bin/spark-sql   --deploy-mode cluster --master yarn --queue None   \
--conf spark.driver.memory=310g \
--conf spark.driver.cores=32 \
--conf spark.driver.maxResultSize=20g \
--conf spark.executor.memory=4g \
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=50g \
--conf spark.executor.memoryOverhead=4G \
--conf spark.reducer.maxBlocksInFlightPerAddress=4 \
--conf spark.reducer.maxReqsInFlight=16 \
--conf spark.dynamicAllocation.maxExecutors=594 \
--conf spark.executor.cores=4 \
--conf spark.sql.shuffle.partitions=4752 \
--conf spark.gluten.sql.columnar.backend.lib=velox \
--conf spark.gluten.sql.columnar.backend.velox.memoryCapRatio=0.75 \
--conf spark.gluten.sql.columnar.coalesce.batches=true \
--conf spark.gluten.sql.columnar.forceshuffledhashjoin=true \
--conf spark.gluten.sql.columnar.maxBatchSize=4096 \
--conf spark.sql.parquet.columnarReaderBatchSize=4096 \
--conf spark.plugins=org.apache.gluten.GlutenPlugin \
--conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
--conf spark.sql.optimizer.runtime.bloomFilter.enabled=true \
--conf spark.driver.extraClassPath=gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
--conf spark.executor.extraClassPath=gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.shuffleTracking.enabled=true \
--conf spark.hadoop.fs.s3a.use.instance.credentials=true \
--conf spark.gluten.sql.columnar.shuffle.codec=zstd \
--conf spark.gluten.sql.columnar.backend.velox.spillStrategy=auto \
--conf spark.gluten.sql.native.writer.enabled=true \
--conf spark.sql.hive.convertMetastoreOrc=true \
--conf spark.executorEnv.MALLOC_CONF=background_thread:true,dirty_decay_ms:10000,muzzy_decay_ms:10000,invalid_flag:foo \
--conf spark.task.maxFailures=2 \
--conf spark.excludeOnFailure.task.maxTaskAttemptsPerNode=1 \
--conf spark.excludeOnFailure.stage.maxFailedTasksPerExecutor=1 \
--conf spark.excludeOnFailure.stage.maxFailedExecutorsPerNode=1 \
--conf spark.excludeOnFailure.application.maxFailedTasksPerExecutor=1 \
--conf spark.excludeOnFailure.application.maxFailedExecutorsPerNode=1 \
--conf spark.excludeOnFailure.application.fetchFailure.enabled=true \
--conf spark.stage.maxConsecutiveAttempts=1 \
--conf spark.excludeOnFailure.enabled=true \
--conf spark.executor.heartbeatInterval=30s \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.sql.sources.outputCommitterClass=com.netflix.bdp.s3.S3DirectoryOutputCommitter \
--conf spark.gluten.sql.native.writer.enabled=true \
--conf spark.gluten.sql.columnar.backend.velox.IOThreads=24 \
--conf spark.gluten.sql.columnar.backend.velox.prefetchRowGroups=1 \
--conf spark.gluten.sql.columnar.backend.velox.cacheEnabled=false \
--conf spark.gluten.sql.columnar.backend.velox.memCacheSize=134217728 \
--conf spark.gluten.sql.columnar.backend.velox.ssdCacheSize=0 \
--conf spark.gluten.sql.columnar.backend.velox.maxCoalescedDistanceBytes=1048576 \
--conf spark.gluten.sql.columnar.backend.velox.loadQuantum=268435456 \
--conf spark.gluten.sql.columnar.backend.velox.maxCoalescedBytes=67108864 \
--conf spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver=0 \
--conf spark.unsafe.exceptionOnMemoryLeak=false \
--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.sql.catalog.iceberg.type=hive \
--conf spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.sql.catalog.spark_catalog.table-default.s3.multipart.part-size-bytes=128MB \
--conf spark.sql.catalog.spark_catalog.table-default.write.distribution-mode=none \
--conf spark.sql.catalog.spark_catalog.table-default.write.parquet.compression-codec=zstd \
--conf spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false \
--conf spark.gluten.sql.columnar.backend.velox.MaxSpillRunRows=1000000000 \
--conf spark.gluten.sql.columnar.backend.velox.maxSpillFileSize=10000000000 \
--conf spark.hadoop.fs.s3a.connection.maximum=5000 \
--conf spark.hadoop.fs.s3.maxConnections=5000 \
--conf spark.gluten.sql.columnar.wholeStage.fallback.threshold=-1 \
--conf spark.gluten.sql.columnar.fallback.ignoreRowToColumnar=true \
--conf spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver=0 \
--conf spark.hadoop.fs.s3a.retry.limit=5 \
--conf spark.gluten.velox.fs.s3a.retry.mode=adaptive \
--conf spark.gluten.sql.columnar.backend.velox.MaxSpillBytes=1099511627776 \
--conf spark.sql.parquet.enableVectorizedReader=false \
--conf spark.gluten.sql.complexType.scan.fallback.enabled=false \
--conf spark.gluten.sql.columnar.backend.velox.minBatchSizeForShuffle=512 \
--conf spark.executor.extraJavaOptions="-Dfile.encoding=UTF-8 -Dsun.net.inetaddr.ttl=5 -Djava.net.preferIPv4Stack=true -XX:+PerfDisableSharedMem -XX:MaxDirectMemorySize=6g -Dlog4j.configuration=file:log4j.properties" \
--conf spark.driver.extraJavaOptions="-Dfile.encoding=UTF-8 -Dsun.net.inetaddr.ttl=5 -Djava.net.preferIPv4Stack=true -XX:+PerfDisableSharedMem -Dlog4j.configuration=file:log4j.properties" \
--conf
spark.files=velox/log4j.properties,velox/libmyudf.so \
--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=libmyudf.so \
--conf spark.gluten.sql.columnar.shuffle.sort.partitions.threshold=4000 \
--conf spark.gluten.sql.columnar.shuffle.sort.columns.threshold=80 \
--name ${1}_velox_run1 \
--jars
velox/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar     --queue None   \
-f velox_test_0111/${1}/${1}.sql

#--conf spark.maxRemoteBlockSizeFetchToMem=6442450944 \
#--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=libmyudf.so \
#--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=libmyudf.so \
#--conf spark.gluten.sql.columnar.backend.velox.glogSeverityLevel=0 \
#--conf spark.gluten.sql.columnar.backend.velox.orc.scan.enabled=false \
#--conf spark.gluten.sql.columnar.batchscan=false \
#--conf spark.gluten.sql.columnar.filescan=false \
#--conf spark.gluten.sql.columnar.hivetablescan=false \
#--conf spark.gluten.sql.debug=true \
#--conf spark.gluten.sql.benchmark_task.stageId=0 \
#--conf spark.gluten.sql.benchmark_task.partitionId=-1 \
#--conf spark.gluten.sql.benchmark_task.taskId=-1 \
#--conf spark.gluten.saveDir=/data/nvme1n1/binwei/ \
#--conf spark.gluten.velox.awsSdkLogLevel=DEBUG \

sqloutput=$?

appid=`yarn application -list -appStates ALL 2>&1 | grep ${1}_velox | grep -E "application_[0-9]{13}_[0-9]{4}"  | sort -r | head -n 1 | awk 'BEGIN{FS="\t"}{print $1}'`

python ./monitor.py stop ${1}_${appid}

#if [ '$2'='skip_profile' ]; then exit; fi

exit

echo sqloutput=$sqloutput

if [ $sqloutput -eq 0 ]; then
	papermill ~/ipython/analysis.ipynb -p appid  ${1}_${appid} ~/ipython/run/velox_${1}_${appid}.ipynb
fi

