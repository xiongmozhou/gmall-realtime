package com.atguigu.gmall.dw.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.dw.realtime.bean.StartUpLog
import com.atguigu.gmall.dw.realtime.constant.GmallConstant
import com.atguigu.gmall.dw.realtime.util.{MyKafkaUtil, RedisUtil}
import com.atguigu.gmall1111.common.util.MyEsUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

//TODO 需求，求出所有的adu，单日访问量，按照mid进行区分。
object DauApp {
    def main(args: Array[String]): Unit = {

        //第一步：获取streamingContext
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau")
        val context = new StreamingContext(conf,Seconds(5))

        //第二步：从kafka中消费数据
        val kafkaDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,context)

        //第三步：我们把数据转换为我们需要的格式
        val startUpDS: DStream[StartUpLog] = kafkaDS.map(record => {
            val value: String = record.value()
            val startUpJson: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
            //我们还有三个时间字段空着，也要填补进去。
            val time = startUpJson.ts
            val currtime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(time))
            val strTimeArray = currtime.split(" ")
            startUpJson.logDate = strTimeArray(0)
            startUpJson.logHour = strTimeArray(1).split(":")(0)
            startUpJson.logHourMinute = strTimeArray(1)
            startUpJson
        })

        //第一次去重，如果已经在redis里面存在了，就不需要进行下面操作，减小IO
        val filterStartUpDS: DStream[StartUpLog] = startUpDS.transform(rdd => {
            println("过滤前：" + rdd.count())
            //使用广播变量来决解序列化问题，然后把数据发送给exerutor。
            val client: Jedis = RedisUtil.getJedisClient
            val key = "dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            val set: util.Set[String] = client.smembers(key)
            val setDataRedis: Broadcast[util.Set[String]] = context.sparkContext.broadcast(set)
            val filterRDD: RDD[StartUpLog] = rdd.filter(x => {
                !setDataRedis.value.contains(x.mid)
            })
            println("过滤一后：" + filterRDD.count())
            filterRDD
        })

        //防止第一个批次出现多个有重复的数据，因为我们后面要往es里面存，所有先做去重处理。
//        val groupByMid: DStream[(String, Iterable[StartUpLog])] = filterStartUpDS.map(x=>(x.mid,x)).groupByKey()
        val groupByMid: DStream[(String, Iterable[StartUpLog])] = filterStartUpDS.map {
            case start => {
//                println("==========")
                (start.mid, start)
            }
        }.groupByKey()
        val flatMapDS: DStream[StartUpLog] = groupByMid.flatMap(x=>x._2)


        //我们把数据存储到redis里面去
        flatMapDS.foreachRDD(rdd=>{
            //这里是在driver执行,因为startUpDS不是rdd
            rdd.foreachPartition(iter=>{
                //这里是在exerutor执行
                val startupLogList: List[StartUpLog] = iter.toList
                val client: Jedis = RedisUtil.getJedisClient
                for (elem <- startupLogList) {
                    //先获取当前时间
                    val key = "dau:"+elem.logDate
                    client.sadd(key,elem.mid)
                }
                //关闭redis连接
                client.close()
                MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_DAU,startupLogList)
            })
        })


        //最后：保证context一直在运行
        context.start()
        context.awaitTermination()
    }
}
