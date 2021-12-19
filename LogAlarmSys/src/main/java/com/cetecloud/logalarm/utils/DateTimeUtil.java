package com.cetecloud.logalarm.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Author: lslzz
 * Date: 2021/9/22
 * Desc: 日期转换工具类
 * 注意：
 *  SimpleDateFormat存在线程安全的问题
 *  从jdk1.8之后，推荐使用DateTimeFormatter、LocalDateTime 、Instant类
 *  替代以前的日期类  SimpleDateFormat、Date、Calendar
 */
public class DateTimeUtil {
    //private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    //将日期对象转换为符串日期
    public static String toYmdhms(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    //将字符串日期转换为时间戳 毫秒数
    public static Long toTs(String dateStr){
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, dtf);
        Long ts = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return ts;
    }

    public static void main(String[] args) {
        System.out.println(ZoneId.systemDefault());
    }
}
