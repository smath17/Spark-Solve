# SparkSolve

Master thesis of moving the partitioning functionality of SolveDB to Spark

# Spark setup

Make sure to use java 8 - https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html  
Download spark-3.1.2-bin-hadoop3.2  
Download Hadoop-3.2.2 (Cdarlint GitHub)

Set system variables:
1. HADOOP_HOME : C:\hadoop (dir)
2. Edit Path and add %HADOOP_HOME%\bin
3. SPARK_HOME : C:\spark-3.1.2-bin-hadoop3.2 (dir)
4. Edit Path and add %SPARK_HOME%\bin

# Optimus & LPSolve setup
In order to use Optimus (and LPSolve) the solver binaries must be available on the system path. This can be accomplished
by following https://github.com/vagmcs/Optimus/blob/master/docs/building_and_linking.md#optional-lpsolve-installation