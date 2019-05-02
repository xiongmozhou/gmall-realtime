package com.atguigu.gmall1111.canal.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.dw.realtime.constant.GmallConstant;
import com.atguigu.gmall1111.canal.util.MyKafkaSender;
import com.google.common.base.CaseFormat;

import java.util.List;

public class CanalHandler {

    public static void  handle(String tableName , CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList){
        //下单操作
        if("order_info".equals(tableName)&& CanalEntry.EventType.INSERT==eventType){
            //遍历行集
            for (CanalEntry.RowData rowData : rowDataList) {
                List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                for (CanalEntry.Column column : columnsList) {
                    System.out.println(column.getName()+"::::"+column.getValue());
                    String propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    jsonObject.put(propertyName,column.getValue());
                }

                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());
            }
        }
    }
}
