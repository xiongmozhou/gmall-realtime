package com.atguigu.gmall.dw.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.dw.realtime.bean.OrderInfo
import com.atguigu.gmall.dw.realtime.constant.GmallConstant
import com.atguigu.gmall.dw.realtime.util.MyKafkaUtil
import com.atguigu.gmall1111.common.util.MyEsUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//单日订单量及收入
object OrderApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
        val context = new StreamingContext(conf,Seconds(3))

        //消息kafka数据，然后存到es里面
        val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,context)

        val orderInfoDstrearm: DStream[OrderInfo] = inputDstream.map {
            _.value()
        }.map { orderJson =>
            val orderInfo: OrderInfo = JSON.parseObject(orderJson, classOf[OrderInfo])
            //日期

            val createTimeArr: Array[String] = orderInfo.createTime.split(" ")
            orderInfo.createDate = createTimeArr(0)
            val timeArr: Array[String] = createTimeArr(1).split(":")
            orderInfo.createHour = timeArr(0)
            orderInfo.createHourMinute = timeArr(0) + ":" + timeArr(1)
            // 收件人 电话 脱敏
            orderInfo.consigneeTel = "*******" + orderInfo.consigneeTel.splitAt(7)._2
            orderInfo
        }

        //保存到ES中
        orderInfoDstrearm.foreachRDD{rdd=>
            rdd.foreachPartition{ orderItr:Iterator[OrderInfo]=>
                val list: List[OrderInfo] = orderItr.toList
                MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_ORDER,list)
            }
        }

        context.start()
        context.awaitTermination()
    }
}
