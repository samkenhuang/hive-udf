package com.huangxin.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import com.huangxin.entity.kol.Interest;

/**
 * Created by huangxin on 2018/06/21
 */
@Description(name = "PingguInterestFieldTopN", value = "_FUNC_(field_str, topN) - convert string like " +
        "`0:0.8319,1:0.1680,2:0.01`  to target [['3C', '游戏'], ['0.8319', '01680']] array by number 2")
public class PingguInterestFieldTopN  extends UDF {
    public ArrayList<ArrayList<String>> evaluate(String fieldStr, int topN) throws Exception {
        ArrayList<String> pros = new ArrayList<>();
        ArrayList<String> ratios = new ArrayList<>();
        ArrayList<Interest> allPro = new ArrayList<>();
        if (fieldStr != null) {
            String[] pairs = fieldStr.toString().split(",");

            for (int i = 0; i < pairs.length; i ++) {
                String[] kv = pairs[i].split(":");
                if (kv.length == 2) {
                    allPro.add(new Interest(kv[0], kv[1]));
                }
            }
            allPro.sort(Interest::compareTo);

            for (int i = 0; i < allPro.size() && i < topN; i ++) {
                pros.add(allPro.get(i).getInterest());
                ratios.add(allPro.get(i).getRatio().toString());
            }
        }
        return new ArrayList<ArrayList<String>>() {{
            add(pros);
            add(ratios);
        }};
    }
}
