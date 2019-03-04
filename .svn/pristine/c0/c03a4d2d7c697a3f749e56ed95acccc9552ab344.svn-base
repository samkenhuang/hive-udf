package com.huangxin.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by huangxin on 2017/12/15.
 */
@Description(name = "CombineUDF", value = "__FUNC_(default, extend) - combine 2 maps into map OR combine 2 list into map. "
            , extended = "Example: \n"
            + " > SELECT _FUNC_(map1, map2) FROM src; OR SELECT _FUNC_(list1, list2) FROM src;")
public class CombineUDF extends GenericUDF {
    private Category category;
    private StandardListObjectInspector stdListInspector;
    private StandardMapObjectInspector stdMapInspector;
    private ListObjectInspector[] listInspectorList;
    private MapObjectInspector[] mapInspectorList;

    public CombineUDF() {

    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (category == Category.LIST) {
            int currSize = 0;
            Object theList = stdListInspector.create(currSize);
            int lastIdx = 0;
            for (int i = 0; i < args.length; i++) {
                List addList  = listInspectorList[i].getList(args[i].get());
                currSize += addList.size();
                stdListInspector.resize(theList, currSize);

                for (int j = 0; j < addList.size(); j++) {
                    Object uninspObj = addList.get(j);
                    Object stdObj = ObjectInspectorUtils.copyToStandardObject(uninspObj, listInspectorList[i].getListElementObjectInspector(), ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                    stdListInspector.set(theList, lastIdx + j, stdObj);
                }
                lastIdx += addList.size();
            }
            return theList;
        } else if (category == Category.MAP) {
            Object theMap = stdMapInspector.create();
            for (int i = 0; i < args.length; ++i) {
                if (args[i].get() != null) {
                    Map addMap = mapInspectorList[i].getMap(args[i].get());
                    for (Object uninspObj : addMap.entrySet()) {
                        Map.Entry uninspEntry = (Entry) uninspObj;
                        Object stdKey = ObjectInspectorUtils.copyToStandardObject(uninspEntry.getKey(), mapInspectorList[i].getMapKeyObjectInspector(), ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                        Object stdVal = ObjectInspectorUtils.copyToStandardObject(uninspEntry.getValue(), mapInspectorList[i].getMapValueObjectInspector(), ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                        stdMapInspector.put(theMap, stdKey, stdVal);
                    }
                }
            }
            return theMap;
        } else {
            throw new HiveException(" Only maps or lists are supports.");
        }
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
        if (arguments.length < 2) {
            throw new UDFArgumentLengthException(
                    "The function CombineUDF(default, extend) take more than 1 argument.");
        }

        ObjectInspector first = arguments[0];
        this.category = first.getCategory();

        if (category == Category.LIST) {
            this.listInspectorList = new ListObjectInspector[arguments.length];
            this.listInspectorList[0] = (ListObjectInspector) first;
            for (int i = 1; i < arguments.length; i ++) {
                ObjectInspector ins = arguments[i];
                if (!ObjectInspectorUtils.compareTypes(first, ins)) {
                    throw new UDFArgumentException("Combine must either be all maps or all lists of the same type");
                }
                this.listInspectorList[i] = (ListObjectInspector) ins;
            }
            this.stdListInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
            return stdListInspector;
        } else if (category == Category.MAP) {
            this.mapInspectorList = new MapObjectInspector[arguments.length];
            this.mapInspectorList[0] = (MapObjectInspector) first;
            for (int i = 0; i < arguments.length; i++) {
                ObjectInspector ins = arguments[i];
                if (!ObjectInspectorUtils.compareTypes(first, ins)) {
                    throw new UDFArgumentException("Combine must either be all maps or all lists of the same type");
                }
                this.mapInspectorList[i] = (MapObjectInspector) ins;
            }
            this.stdMapInspector = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first, ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
            return stdMapInspector;
        } else {
            throw new UDFArgumentException(" combine only takes maps or lists");
        }
    }
}
