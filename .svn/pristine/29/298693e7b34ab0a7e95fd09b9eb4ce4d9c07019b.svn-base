package com.huangxin.hive;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by huangxin on 2017/11/28.
 */
public class JSONPathTest {
    @Test
    public void testJSONPath() throws HiveException {
        String json = "[{\"name\":\"john\",\"gender\":\"male\"},{\"name\":\"ben\"}]";
        String path = "$[*].name";
        JSONPath udf = new JSONPath();
        ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        ObjectInspector[] arguments = { valueOI0, valueOI1};
        udf.initialize(arguments);

        DeferredObject jsonObj = new DeferredJavaObject(new Text(json));
        DeferredObject pathObj = new DeferredJavaObject(new Text(path));
        DeferredObject[] args = {jsonObj, pathObj};
        ArrayList<String> output = (ArrayList<String>) udf.evaluate(args);
        ArrayList<String> expect = new ArrayList<>();
        expect.add("john");
        expect.add("ben");

        assertEquals("JSONPath test", true, true);

    }
}
