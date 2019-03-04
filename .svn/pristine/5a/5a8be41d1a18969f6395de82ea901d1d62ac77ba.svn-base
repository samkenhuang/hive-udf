package com.weiboyi.bi.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import java.math.BigInteger;

/**
 * Created by huangxin on 2018/11/22
 */
@Description(name = "BinaryToBigInteger", value = "_FUNC_(hex_str) - convert binary to BigInteger")
public class BinaryToBigInteger extends UDF {
    public LongWritable evaluate(BytesWritable b) throws Exception {
        if (b == null) return null;
        b.setCapacity(b.getLength());
        BigInteger num = new BigInteger(b.getBytes());
        return new LongWritable(num.longValueExact());
    }
}

