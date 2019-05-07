package com.atguigu.gmall1111.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1111.publisher.bean.Option;
import com.atguigu.gmall1111.publisher.bean.OptionGroup;
import com.atguigu.gmall1111.publisher.bean.SaleInfo;
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

        HashMap<String, Object> orderAmountMap = new HashMap<>();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        Double orderTotalAmount = publisherService.getOrderTotalAmount(date);
        orderAmountMap.put("value",orderTotalAmount);


        listTotal.add(map1);
        listTotal.add(map2);
        listTotal.add(orderAmountMap);
        return JSON.toJSONString(listTotal);
    }

    @GetMapping("realtime-hour")
    public String getRealtimeHourAndCount(@RequestParam("date") String date,@RequestParam("id") String id){
        String hourJson = "";
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
            hourJson = JSON.toJSONString(outMap);
        }else if("order_amount".equals(id)){
            //查询今日分时
            Map orderAmountHourTdMap = publisherService.getOrderTotalAmountHour(date);
            //查询昨日分时
            String yesterday = GetDayUtil.getBeforeDayTime(date, 1);
            Map orderAmountHourYdMap = publisherService.getOrderTotalAmountHour(yesterday);

            HashMap<String, Map> realtimeHourMap = new HashMap<>();
            realtimeHourMap.put("yesterday",orderAmountHourYdMap);
            realtimeHourMap.put("today",orderAmountHourTdMap);
            hourJson=JSON.toJSONString(realtimeHourMap);
        }
        return hourJson;
    }


    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("keyword") String keyword,@RequestParam("startpage") int startPage, @RequestParam("size") int size){
        SaleInfo saleInfoWithGenderAggs = publisherService.getSaleInfo(date, keyword, startPage, size, "user_gender", 2);
        Integer total = saleInfoWithGenderAggs.getTotal();
        Map genderAggsMap = saleInfoWithGenderAggs.getTempAggsMap();

        Long maleCount = (Long)genderAggsMap.getOrDefault("M", 0L);
        Long femaleCount = (Long)genderAggsMap.getOrDefault("F", 0L);

        Double maleRatio=   Math.round(maleCount*1000D/total )/10D;
        Double femaleRatio=   Math.round(femaleCount*1000D/total )/10D;

        List<Option> genderOptionList=new ArrayList<>();  //选项列表
        genderOptionList.add(new Option("男", maleRatio));
        genderOptionList.add(new Option("女", femaleRatio));

        List<OptionGroup> optionGroupList=  new ArrayList<>(); //饼图列表
        optionGroupList.add( new OptionGroup(genderOptionList,"性别占比")) ;



        SaleInfo saleInfoWithAgeAggs = publisherService.getSaleInfo(date, keyword, startPage, size, "user_age", 100);
        Map ageAggsMap = saleInfoWithAgeAggs.getTempAggsMap();
        //通过每个年龄的计数清单 计算各个年龄段的占比

        Long age_20Count=0L;
        Long age20_30Count=0L;
        Long age30_Count=0L;
        for (Object o : ageAggsMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String ageStr =(String) entry.getKey();
            Integer age=Integer.parseInt(ageStr);

            Long count =(Long) entry.getValue();
            if(age<20){
                age_20Count+=count;
            }else if(age>=20&&age<30){
                age20_30Count+=count;
            }else{
                age30_Count+=count;
            }

        }
        Double age_20Ratio=0D;
        Double age20_30Ratio=0D;
        Double age30_Ratio=0D;

        age_20Ratio=Math.round(age_20Count*1000D/total )/10D;
        age20_30Ratio=Math.round(age20_30Count*1000D/total )/10D;
        age30_Ratio=Math.round(age30_Count*1000D/total )/10D;

        List<Option> ageOptionList=new ArrayList<>();  //选项列表
        ageOptionList.add(new Option("小于20岁", age_20Ratio));
        ageOptionList.add(new Option("20至30岁", age20_30Ratio));
        ageOptionList.add(new Option("30岁及以上", age30_Ratio));

        optionGroupList.add( new OptionGroup(ageOptionList,"年龄占比"));

        SaleInfo saleInfo = new SaleInfo();
        saleInfo.setTotal(total);
        saleInfo.setDetail(saleInfoWithGenderAggs.getDetail());
        saleInfo.setStat(optionGroupList);

        return JSON.toJSONString(saleInfo);

    }
}

