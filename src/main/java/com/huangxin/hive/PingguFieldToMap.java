package com.huangxin.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.Map;

/**
 * Created by huangxin on 2018/06/21
 */
@Description(name = "PingguFieldToMap", value = "_FUNC_(field_str, map) - convert string like " +
        "`0:0.831966817379,1:0.168033197522`  to target map('男', 0.168033197522, '女', 0.831966817379) map by map('1', '男', '0', '女')")
public class PingguFieldToMap extends GenericUDF {
    private StandardMapObjectInspector stdMapInspector;

    public PingguFieldToMap() {}

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        Object mapRes = stdMapInspector.create();
        Map map = stdMapInspector.getMap(args[1].get());
        String[] pair = args[0].get().toString().split(",");
        for (int j = 0; j < pair.length; j ++) {
            String[] kv = pair[j].split(":");
            Text key = new Text(kv[0]);
            if (map.containsKey(key)) {
                stdMapInspector.put(mapRes, map.get(key), new Text(kv[1]));
            }
        }
        return mapRes;
    }

    @Override
    public String getDisplayString(String[] args) {
        StringBuilder sb = new StringBuilder("combine( ");
        for (int i = 0; i < args.length - 1; ++i) {
            sb.append(args[i]);
            sb.append(",");
        }
        sb.append(args[args.length - 1]);
        sb.append(")");
        return sb.toString();
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "The function PingguFieldToMap(fieldStr, map) take exactly 2 arguments."
            );
        }
        ObjectInspector keyIns = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        ObjectInspector valueIns = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        this.stdMapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(keyIns, valueIns);

        return stdMapInspector;

    }
}
