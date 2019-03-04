package com.huangxin.entity.kol;

import java.util.HashMap;
import java.util.Map;

public class Interest implements Comparable<Interest> {
    private static Map<String, String> interestMap = new HashMap<String, String>(){{
        put("1", "3C");
        put("2", "游戏");
        put("3", "家居生活");
        put("4", "旅游");
        put("5", "美食");
        put("6", "汽车");
        put("7", "奢侈品");
        put("8", "时尚");
        put("9", "娱乐");
        put("10", "孕产育儿");
    }};
    private String interest;
    private Double ratio;

    public Interest(String interest, String ratio) {
        super();
        this.interest = interest;
        this.ratio = Double.valueOf(ratio);
    }

    public String getInterest() {
        return interestMap.get(interest);
    }
    public void setInterest(String interest) {
        this.interest = interest;
    }
    public Double getRatio() {
        return ratio;
    }
    public void setRatio(Double ratio) {
        this.ratio = ratio;
    }

    // 按照ratio降序
    @Override
    public int compareTo(Interest o) {
        int res = 0;
        res =- this.ratio.compareTo(o.ratio);
        if (res == 0) {
            res = this.interest.compareTo(o.interest);
        }
        return res;
    }
}
