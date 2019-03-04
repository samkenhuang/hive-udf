package com.huangxin.hive;

import java.util.*;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by huangxin on 2017/11/28.
 */
public class CombineUDFTest {

    private CombineUDF udf;

    @Before
    public void before() {
        udf = new CombineUDF();
    }

    /**
     * {1, 2}, {2, 3} = {1, 2, 3}
     * @throws HiveException
     */
    @Test
    public void testCombineUDFList1() throws HiveException {
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        StandardListObjectInspector resultOI = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOI, listOI});

        List<Integer> one = new ArrayList<Integer>(){{
            add(1);
            add(2);
        }};
        List<Integer> two = new ArrayList<Integer>(){{
            add(2);
            add(3);
        }};

        Object result = udf.evaluate(new GenericUDF.DeferredObject[] {new DeferredJavaObject(one), new DeferredJavaObject(two)});
        assertEquals(4, resultOI.getListLength(result));
        assertTrue(resultOI.getList(result).contains(1));
        assertTrue(resultOI.getList(result).contains(2));
        assertTrue(resultOI.getList(result).contains(3));
    }

    /**
     * {}, {2, 3} = {2, 3}
     * @throws HiveException
     */
    @Test
    public void testCombineUDFList2() throws HiveException {
        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(intOI);
        StandardListObjectInspector resultOI = (StandardListObjectInspector) udf.initialize(new ObjectInspector[]{listOI, listOI});

        List<Integer> one = new ArrayList<Integer>(){{
            add(1);
            add(2);
        }};
        List<Integer> two = new ArrayList<Integer>(){{
        }};

        Object result = udf.evaluate(new GenericUDF.DeferredObject[] {new DeferredJavaObject(one), new DeferredJavaObject(two)});
        assertEquals(2, resultOI.getListLength(result));
        assertTrue(resultOI.getList(result).contains(1));
        assertTrue(resultOI.getList(result).contains(2));
    }

    /**
     * {key1: "value1"} , {key2: "value2"} = {key1: "value1", key2:"value2"}
     * @throws HiveException
     */
    @Test
    public void testCombineUDFMap() throws HiveException {
    }
}
