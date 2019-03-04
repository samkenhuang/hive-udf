package com.huangxin.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFJson;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangxin on 2017/11/22.
 */
@Description(name = "JSONPath", value = "_FUNC_(json, json_path) - extract json to array by given jsonPath. "
            , extended = "Example:\n"
            + "  > SELECT _FUNC_(json, json_path) FROM src LIMIT 1;")
public class JSONPath extends GenericUDF {
    private ObjectInspectorConverters.Converter[] converters;

    public JSONPath() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "The function JSONPath(json, json_path) takes exactly 2 arguments.");
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];
        for (int i = 0; i < arguments.length; i ++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }

        return ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
                .writableStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 2);

        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        }

        try {
            Text jsonText = (Text) converters[0].convert(arguments[0].get());
            Text pathText = (Text) converters[1].convert(arguments[1].get());
            String json = jsonText.toString();
            String path = pathText.toString();
            ReadContext ctx = JsonPath.parse(json);
            List<String> list = JsonPath.read(json, path);
            ArrayList<Text> ret = new ArrayList<Text>(list.size());
            ret.add(new Text());
            ret.clear();
            for (String s : list) {
                ret.add(new Text(s));
            }
            return ret;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == 2);
        return "JSONPath(" + strings[0] + ", " + strings[1] + ")";
    }
}
