[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-f059dc9a6f8d3a56e377f745f24479a46679e63a5d9fe6f495e02850cd0d8118.svg)](https://classroom.github.com/online_ide?assignment_repo_id=6017417&assignment_repo_type=AssignmentRepo)
# Spark Sparse Matrix Product using Block-Block partitioning (B-B)

Fall 2021

Code author
-----------
Jackson Neal
Zoheb Nawaz
Zachary Hillman

Installation
------------
These components are installed:
- JDK 1.8
- Scala 2.12.13
- Hadoop 3.3.0
- Spark 3.1.2 (without bundled Hadoop)
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_profile:
```
export HADOOP_HOME=/Users/landonneal/jacksonn/CS6240/hadoop-3.3.0
export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home
export YARN_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export SCALA_HOME=/Users/landonneal/jacksonn/CS6240/scala-2.12.13
export SPARK_HOME=/Users/landonneal/jacksonn/CS6240/spark-3.1.2-bin-without-hadoop
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

export PATH=$HADOOP_HOME:$PATH
export PATH=/opt/homebrew/bin:$PATH
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PATH=$SCALA_HOME/bin:$PATH
```

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
```
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
```

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
5) Standalone Hadoop:
    - make switch-standalone		-- set standalone Hadoop environment (execute once)
    - make local
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
    - make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
    - make pseudo					-- first execution
    - make pseudoq				-- later executions since namenode and datanode already running 
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
    - make upload-input-aws		-- only before first execution
    - make aws					-- check for successful execution with web interface (aws.amazon.com)
    - download-output-aws			-- after successful execution & termination
