package com.atguigu.gmall1111.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1111.publisher.service.PublisherService;
import com.atguigu.gmall1111.publisher.util.GetDayUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date){
        int dauTotal = publisherService.getDauTotal(date);

        List<Map> listTotal = new ArrayList<>();
        HashMap<String, Object> map1 = new HashMap<>();
        HashMap<String, Object> map2 = new HashMap<>();

        map1.put("id","dau");
        map1.put("name","新增日活");
        map1.put("value",dauTotal);

        map2.put("id","new_mid");
        map2.put("name","c");
        map2.put("value",233);

        listTotal.add(map1);
        listTotal.add(map2);

        return JSON.toJSONString(listTotal);
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHourAndCount(@RequestParam("date") String date,@RequestParam("id") String id){
        //进行判断类型
        if("dau".equals(id)){
            //查询数据
            Map todayHours = publisherService.getDauHours(date);
            String beforeDayTime = GetDayUtil.getBeforeDayTime(date, 1);
            Map yesterdayHours = publisherService.getDauHours(beforeDayTime);
            //定义两个map来存放数据
            //{"yesterday":{"钟点":数量},"today":{"钟点":数量}}
            HashMap<String, Map> outMap = new HashMap<>();

            //循环遍历数据，然后放到map中
            outMap.put("yesterday",yesterdayHours);
            outMap.put("today",todayHours);
            return JSON.toJSONString(outMap);
        }else {
            return "请正确输入参数，sorry，查询不到有效信息。";
        }
    }
}

