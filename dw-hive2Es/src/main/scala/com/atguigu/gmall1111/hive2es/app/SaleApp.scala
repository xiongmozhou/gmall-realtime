package com.atguigu.gmall1111.hive2es.app

import com.atguigu.gmall1111.common.constant.GmallConstant
import com.atguigu.gmall1111.common.util.MyEsUtil
import com.atguigu.gmall1111.hive2es.bean.SaleDetailDaycount
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object SaleApp {
//shell : spark-submit    xxxxxxxxxxxxxxx     .jar      xxx
  def main(args: Array[String]): Unit = {
    var date=""
    if(args.length>0){
      val date = args(0)
    }else{
      date="2019-02-01"
    }
    val sparkConf: SparkConf = new SparkConf().setAppName("sale_app").setMaster("local[*]")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    // 读取hive 的宽表
    sparkSession.sql("use gmall0925")
    import sparkSession.implicits._
    val saleRdd: RDD[SaleDetailDaycount] = sparkSession.sql("select user_id,sku_id,user_gender,cast(user_age as int) user_age,user_level,cast(sku_price as double),sku_name,sku_tm_id, sku_category3_id,sku_category2_id," +
      "sku_category1_id,sku_category3_name,sku_category2_name,sku_category1_name,spu_id,sku_num,cast(order_count as bigint) order_count,cast(order_amount as double) order_amount,dt " +
      "from dws_sale_detail_daycount where dt='"+date+"'").as[SaleDetailDaycount].rdd

    // 往es中写入
    saleRdd.foreachPartition{ saleItr=>
      var i=0
      val listBuffer: ListBuffer[SaleDetailDaycount] =new  ListBuffer
      for (saleDetail <- saleItr ) {
        listBuffer+=saleDetail
        i+=1
        //达到100进行批量保存
        if(i%100==0){
          MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_SALE, listBuffer.toList)
          listBuffer.clear()
        }
      }
      //零头 批量保存
      if(listBuffer.size>0){
        MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_SALE, listBuffer.toList)
      }

    }


  }

}
