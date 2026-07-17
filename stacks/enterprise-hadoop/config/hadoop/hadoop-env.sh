export LANG=en_US.UTF-8
export HADOOP_OS_TYPE=${HADOOP_OS_TYPE:-$(uname -s)}
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
export HADOOP_HOME=/etc/hadoop-3.4.1
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_CONF_DIR=/etc/hadoop-3.4.1/etc/hadoop
export HADOOP_OPTS=-Djava.library.path=$HADOOP_HOME/lib/native
export HIVE_HOME=/opt/apache-hive-4.0.1-bin
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH=/etc/hadoop-3.4.1/lib/native:$DYLD_LIBRARY_PATH
export YARN_CONF_DIR=/etc/hadoop-3.4.1/etc/hadoop
export CLASSPATH=/usr/lib/jvm/java-1.8.0-openjdk-amd64/lib:/etc/hadoop-3.4.1/share/hadoop/common/lib:/etc/hadoop-3.4.1/share/hadoop/yarn:/etc/hadoop-3.4.1/share/hadoop/yarn/lib:/etc/hadoop-3.4.1/share/hadoop/mapreduce:/etc/hadoop-3.4.1/share/hadoop/client:/etc/hadoop-3.4.1/share/hadoop/hdfs:/opt/apache-hive-4.0.1-bin/lib/*:/opt/spark-3.4.4-bin-hadoop3/jars/*`$HADOOP_HOME/bin/hdfs classpath --glob`
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HIVE_HOME/bin:/home/hadoop/.local/bin:$SPARK_HOME/bin
export SPARK_CLASSPATH=$HIVE_HOME/lib/*
export HIVE_CONF_DIR=/opt/apache-hive-4.0.1/conf
export HUDI_CONF_DIR=/etc/hudi/conf
export SPARK_HOME=/opt/spark-3.4.4-bin-hadoop3
export SPARK_CONF_DIR=/opt/spark-3.4.4-bin-hadoop3/conf
 
 
export TEZ_HOME=/opt/apache-tez-0.10.4-bin
export PATH=$PATH:$TEZ_HOME/bin
export TEZ_CONF_DIR=$TEZ_HOME/conf
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$TEZ_HOME/*:$TEZ_HOME/lib/*
export SPARK_CLASSPATH=$HIVE_HOME/lib/*
export HIVE_LOG4J_FILE=/opt/apache-hive-4.0.1-bin/conf/hive-log4j.properties

