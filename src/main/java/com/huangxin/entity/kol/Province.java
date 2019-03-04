package com.huangxin.entity.kol;

import java.util.HashMap;
import java.util.Map;

public class Province implements Comparable<Province> {

    private static Map<String, String> provinceMap = new HashMap<String, String>(){{
        put("anhui", "安徽");
        put("beijing", "北京");
        put("chongqing", "重庆");
        put("fujian", "福建");
        put("gansu", "甘肃");
        put("guangdong", "广东");
        put("guangxi", "广西");
        put("guizhou", "贵州");
        put("hainan", "海南");
        put("hebei", "河北");
        put("heilongjiang", "黑龙江");
        put("henan", "河南");
        put("hubei", "湖北");
        put("hunan", "湖南");
        put("jiangsu", "江苏");
        put("jiangxi", "江西");
        put("jilin", "吉林");
        put("liaoning", "辽宁");
        put("neimenggu", "内蒙古");
        put("ningxia", "宁夏");
        put("qinghai", "青海");
        put("shandong", "山东");
        put("shanghai", "上海");
        put("shanxi", "山西");
        put("shanxisheng", "陕西");
        put("sichuan", "四川");
        put("tianjin", "天津");
        put("xinjiang", "新疆");
        put("xizang", "西藏");
        put("yunnan", "云南");
        put("zhejiang", "浙江");
    }};
    private String province;
    private Double ratio;

    public Province(String province, String ratio) {
        super();
        this.province = province;
        this.ratio = Double.valueOf(ratio);
    }

    public String getProvince() {
        return provinceMap.get(province);
    }
    public void setProvince(String province) {
        this.province = province;
    }
    public Double getRatio() {
        return ratio;
    }
    public void setRatio(Double ratio) {
        this.ratio = ratio;
    }

    // 按照ratio降序
    @Override
    public int compareTo(Province o) {
        int res = 0;
        res =- this.ratio.compareTo(o.ratio);
        if (res == 0) {
            res = this.province.compareTo(o.province);
        }
        return res;
    }
}
