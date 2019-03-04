package com.huangxin.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import com.huangxin.entity.kol.Province;


/**
 * Created by huangxin on 2018/06/21
 */
@Description(name = "PingguProvinceFieldTopN", value = "_FUNC_(field_str, topN) - convert string like " +
        "`anhui:0.8319,chongqqing:0.1680,jiangxi:0.01`  to target [['安徽', '重庆'], ['0.8319', '01680']] array by number 2")
public class PingguProvinceFieldTopN extends UDF {
    public ArrayList<ArrayList<String>> evaluate(String fieldStr, int topN) throws Exception {
        ArrayList<String> pros = new ArrayList<>();
        ArrayList<String> ratios = new ArrayList<>();
        ArrayList<Province> allPro = new ArrayList<>();
        if (fieldStr != null) {
            String[] pairs = fieldStr.toString().split(",");
            for (int i = 0; i < pairs.length; i ++) {
                String[] kv = pairs[i].split(":");
                if (kv.length == 2) {
                    allPro.add(new Province(kv[0], kv[1]));
                }
            }
            allPro.sort(Province::compareTo);

            for (int i = 0; i < allPro.size() && i < topN; i ++) {
                pros.add(allPro.get(i).getProvince());
                ratios.add(allPro.get(i).getRatio().toString());
            }
        }
        return new ArrayList<ArrayList<String>>() {{
            add(pros);
            add(ratios);
        }};
    }
}

