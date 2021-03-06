# HuangXin Hive UDFs

## About
HuangXin Hive UDFs is a collection of user defined functions for Hive.

## Quickstart
    $ git clone https://github.com/samkenhuang/hive-udfs.git
    $ cd hive-udfs
    $ mvn clean package

## How to use
    hive> add jar hdfs://master110.weiboyi.com:8020/user/bi/libs/huangxin-hive-1.0-SNAPSHOT-shaded.jar;

## support functions

#### JSONPath
    hive> CREATE temporary function JSONPath as 'com.huangxin.hive.JSONPath';
    hive> select JSONPath("[{\"name\":\"john\",\"gender\":\"male\"},{\"name\":\"ben\"}]", "$[*].name");
See the details at [github:json-path/JsonPath](https://github.com/json-path/JsonPath)

#### bi_to_biginteger
    hive> drop function bi_to_biginteger;
    hive> create function bi_to_biginteger as 'com.weiboyi.bi.hive.udf.BinaryToBigInteger' using jar 'hdfs://wbyhdfs/user/huangxin/jars/bi-hive-udf-1.0.jar';


## License
[Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
