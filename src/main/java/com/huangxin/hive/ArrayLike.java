package com.huangxin.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

import java.util.regex.*;

/**。。
 * Created by huangxin on 2018/03/22
 */
@Description(name = "ArrayLike", value = "_FUNC_(Array, string, MODE) - check if the string is similar to the element in the array. MODE could be (URL)"
, extended = "Example:\n"
        + " > select _FUNC_(array('https://weibo.com/1234'), 'http://weibo.com/1234', 'URL'); \n"
        + "return true. " +
        "URL mode means ignore the protocol and the params it takes, it will precisely compare." +
        "DEFAULT mode will be work as like.")
public class ArrayLike extends GenericUDF {

    private static final int ARRAY_IDX = 0;
    private static final int VALUE_IDX = 1;
    private static final int MODE_IDX = 2;
    private static final int ARG_COUNT = 3;
    private static final String FUNC_NAME = "ARRAY_LIKE";

    private ObjectInspector valueOI;
    private ListObjectInspector arrayOI;
    private ObjectInspector arrayElementOI;
    private ObjectInspector modeOI;
    private BooleanWritable result;


    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Check if two argument were passed
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentException("The function " + FUNC_NAME + " accepts " + ARG_COUNT + " arguments.");
        }
        // Check if ARRAY_IDX argument is of category LIST
        if (!arguments[ARRAY_IDX].getCategory().equals(Category.LIST)) {
            throw new UDFArgumentTypeException(ARRAY_IDX, "\"" + "LIST"
                + "\" " + "expected at function " + FUNC_NAME + ", but " + "\"" + arguments[ARRAY_IDX].getTypeName()
                + "\" is found");
        }

        arrayOI = (ListObjectInspector) arguments[ARRAY_IDX];
        arrayElementOI = arrayOI.getListElementObjectInspector();
        valueOI = arguments[VALUE_IDX];
        modeOI = arguments[MODE_IDX];
        result = new BooleanWritable(false);

        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        result.set(false);
        Object array = arguments[ARRAY_IDX].get();
        Object value = arguments[VALUE_IDX].get();
        Object mode = arguments[MODE_IDX].get();

        int arrayLength = arrayOI.getListLength(array);

        if (value == null || arrayLength <= 0) {
            return result;
        }
        String valueElement = value.toString();
        String modeElement = mode.toString();

        if (modeElement.equals("URL")) {
            Pattern p = Pattern.compile("^(http://|https://){0,1}([^?]*)");
            Matcher m = p.matcher(valueElement);
            if (!m.find() || m.groupCount() != 2) return result;
            String search = m.group(2);
            for (int i = 0; i < arrayLength; i ++) {
                String listElement = arrayOI.getListElement(array, i).toString();
                Matcher m1 = p.matcher(listElement);
                if (m1.find() && m1.groupCount() == 2) {
                    if (m1.group(2).equals(search)) {
                        result.set(true); break;
                    }
                }

            }
        } else if (modeElement.equals("DEFAULT")){
            for (int i = 0; i < arrayLength; i ++) {
                String listElement = arrayOI.getListElement(array, i).toString();
                if (listElement.indexOf(valueElement) != -1) {
                    result.set(true); break;
                }
            }
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert(children.length == ARG_COUNT);
        return "array_like(" + children[ARRAY_IDX] + "," + children[VALUE_IDX] + "," + children[MODE_IDX] +")";
    }
}
