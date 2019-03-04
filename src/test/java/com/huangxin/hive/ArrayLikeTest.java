package com.huangxin.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

public class ArrayLikeTest {
    private ArrayLike udf;
    private BooleanObjectInspector resultOI;

    @Before
    public void before() {
        udf = new ArrayLike();
        try {
            ObjectInspector stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(stringOI);

            ObjectInspector[] arguments = {listOI, stringOI, stringOI};
            udf.initialize(arguments);
            resultOI = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;

        } catch (UDFArgumentException e) {
        }
    }

    @Test
    public void testUrlTrue() throws HiveException {
        List<String> list = new ArrayList<String>(){{
            add("http://weibo.com/1234");
            add("http://weibo.com/123");
        }};
        String value = "weibo.com/1234";
        String mode = "URL";

        BooleanWritable result = (BooleanWritable) udf.evaluate(new DeferredObject[] {
                new DeferredJavaObject(list),
                new DeferredJavaObject(value),
                new DeferredJavaObject(mode)
        });
        assertEquals(true, result.get());

        List<String> list1 = new ArrayList<String>(){{
            add("http://weibo.com/1234?test=true");
            add("http://weibo.com/123");
        }};
        String value1 = "weibo.com/1234";
        String mode1 = "URL";

        BooleanWritable result1 = (BooleanWritable) udf.evaluate(new DeferredObject[] {
                new DeferredJavaObject(list1),
                new DeferredJavaObject(value1),
                new DeferredJavaObject(mode1)
        });
        assertEquals(true, result1.get());
    }

    @Test
    public void testUrlFalse() throws HiveException {
        List<String> list = new ArrayList<String>(){{
            add("http://weibo.com/1234?note=false");
            add("http://weibo.com/123");
        }};
        String value = "weibo.com/12345";
        String mode = "URL";

        BooleanWritable result = (BooleanWritable) udf.evaluate(new DeferredObject[] {
                new DeferredJavaObject(list),
                new DeferredJavaObject(value),
                new DeferredJavaObject(mode)
        });
        assertEquals(false, result.get());
    }

    @Test
    public void testDefaultTrue() throws HiveException {
        List<String> list = new ArrayList<String>(){{
            add("黄鑫lalal");
            add("http://weibo.com/123");
        }};
        String value = "黄鑫la";
        String mode = "DEFAULT";

        BooleanWritable result = (BooleanWritable) udf.evaluate(new DeferredObject[] {
                new DeferredJavaObject(list),
                new DeferredJavaObject(value),
                new DeferredJavaObject(mode)
        });
        assertEquals(true, result.get());
    }

    @Test
    public void testDefaultFalse() throws HiveException {
        List<String> list = new ArrayList<String>(){{
            add("黄鑫lalal");
            add("http://weibo.com/123");
        }};
        String value = "黄鑫lall";
        String mode = "DEFAULT";

        BooleanWritable result = (BooleanWritable) udf.evaluate(new DeferredObject[] {
                new DeferredJavaObject(list),
                new DeferredJavaObject(value),
                new DeferredJavaObject(mode)
        });
        assertEquals(false, result.get());
    }
}
