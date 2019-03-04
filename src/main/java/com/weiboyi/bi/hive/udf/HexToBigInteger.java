package com.weiboyi.bi.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;

import java.math.BigInteger;

/**
 * Created by huangxin on 2018/11/22
 */
@Description(name = "HexToBigInteger", value = "_FUNC_(hex_str) - convert hex string to BigInteger like " +
        " HexToBigInteger(\"FEDF1D\") to -73955")
public class HexToBigInteger extends UDF {
    public LongWritable evaluate(String hex) throws Exception {
        if (hex == null) return null;
        byte[] bytes = hexToByteArray(hex);
        BigInteger num = new BigInteger(bytes);
        return new LongWritable(num.intValue());
    }

    private static byte[] hexToByteArray(String inHex) {
        int hexlen = inHex.length();
        byte[] result;
        if (hexlen % 2 == 1){
            //奇数
            hexlen++;
            result = new byte[(hexlen/2)];
            inHex="0"+inHex;
        } else {
            //偶数
            result = new byte[(hexlen/2)];
        }
        int j=0;
        for (int i = 0; i < hexlen; i+=2){
            result[j]=hexToByte(inHex.substring(i,i+2));
            j++;
        }
        return result;
    }

    private static byte hexToByte(String inHex) {
        return (byte)Integer.parseInt(inHex,16);
    }
}
