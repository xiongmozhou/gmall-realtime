package com.atguigu.gmall1111.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall1111.common.constant.GmallConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    public static final Logger LOGGER = LoggerFactory.getLogger(LoggerController.class);

    @PostMapping("log")
    public String doLog(@RequestParam("log") String log) {
        //获取json对象，
        JSONObject jsonObject = JSON.parseObject(log);
        //给每条数据添加一个字段，时间戳，因为时间还是要以服务器为准
        jsonObject.put("ts",System.currentTimeMillis());
        // 落盘成为日志文件 ==> log4j
        LOGGER.info(jsonObject.toJSONString());
        //发送到kafka里面，根据事件名称进行发往不同主题
        if ("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());
        }else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());
        }
        return "success";
    }
}
