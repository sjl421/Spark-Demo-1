/usr/local/spark/bin/spark-submit \
--class cn.ibeifeng.spark.SparkSQLDemo \
--master spark://spark2upgrade01:7077 \
--num-executors 1 \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 1 \
/usr/local/test_spark_app/spark2-upgrade-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
