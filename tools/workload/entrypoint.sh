
# Change owner of files added on top of Spark installation
sudo chown -R spark:spark "${SPARK_HOME}"


${SPARK_HOME}/sbin/start-master.sh
${SPARK_HOME}/sbin/start-worker.sh spark://localhost:7077

/usr/bin/telegraf &

jupyter notebook --ip=0.0.0.0 --port=8989 --no-browser --notebook-dir=/opt/spark/work-dir/ipython