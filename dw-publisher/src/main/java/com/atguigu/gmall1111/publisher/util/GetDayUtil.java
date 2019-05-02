package com.atguigu.gmall1111.publisher.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GetDayUtil {

    /**此函数是获取日期的前一天或日期
     * @param today  传入一个日期
     * @param num 根据传入的日期进行相减
     * @return 返回最近得到日期
     */
    public static String getBeforeDayTime(String today,Integer num){
        String beforeTime = "";
        try {
            long time = new SimpleDateFormat("yyyy-MM-dd").parse(today).getTime();
            beforeTime = new SimpleDateFormat("yyyy-MM-dd").format(new Date(time-(num*24*60*60*1000)));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return beforeTime;
    }
}
