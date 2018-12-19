package com.cuteximi.spark.bean;

import java.io.Serializable;

/**
 * @program: sparktest2
 * @description: 天气的bean
 * @author: TSL
 * @create: 2018-12-18 16:58
 * 实现序列化接口
 **/
public class Weather implements Serializable {


    private String year;
    private String month;
    private String day;
    private String time;

    private String wenDu;

    public String getWenDu() {
        return wenDu;
    }

    public void setWenDu(String wenDu) {
        this.wenDu = wenDu;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
