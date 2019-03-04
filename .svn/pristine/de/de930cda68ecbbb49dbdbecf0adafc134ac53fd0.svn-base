#!/usr/bin/env bash
/usr/local/program/apache-maven-3.5.2/bin/mvn clean package
hadoop dfs -fs hdfs://prd-pg-cdh-node-193:8020 -rm jars/bi-hive-udf-1.0.jar
hadoop dfs -fs hdfs://prd-pg-cdh-node-193:8020 -copyFromLocal target/bi-hive-udf-1.0.jar jars