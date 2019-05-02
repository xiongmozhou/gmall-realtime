package com.atguigu.gmall1111.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.gmall1111.canal.handler.CanalHandler;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {


    public static void main(String[] args) {
        //连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");
        while (true){
            canalConnector.connect();
            canalConnector.subscribe("realtimemall.order_info");
            Message message = canalConnector.get(100);
            int size = message.getEntries().size();

            if(size==0){
                System.out.println("没有数据！！休息5秒");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }else{
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if(  entry.getEntryType()== CanalEntry.EntryType.ROWDATA  ){

                        CanalEntry.RowChange rowChange=null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        String tableName = entry.getHeader().getTableName();// 表名
                        CanalEntry.EventType eventType = rowChange.getEventType();//insert update delete？
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();//行集  数据
                        CanalHandler.handle(tableName,eventType,rowDatasList);
                    }
                }
            }
        }
    }
}
