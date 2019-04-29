package com.atguigu.gmall1111.common.util

import java.util
import java.util.Objects

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.dw.realtime.util.Movie
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

import scala.collection.immutable.HashMap

object MyEsUtil {
    private val ES_HOST = "http://hadoop102"
    private val ES_HTTP_PORT = 9200
    private var factory:JestClientFactory = null

    /**
      * 获取客户端
      *
      * @return jestclient
      */
    def getClient: JestClient = {
        if (factory == null) build()
        factory.getObject
    }

    /**
      * 关闭客户端
      */
    def close(client: JestClient): Unit = {
        if (!Objects.isNull(client)) try
            client.shutdownClient()
        catch {
            case e: Exception =>
                e.printStackTrace()
        }
    }

    /**
      * 建立连接
      */
    private def build(): Unit = {
        factory = new JestClientFactory
        factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
          .maxTotalConnection(20) //连接总数
          .connTimeout(10000).readTimeout(10000).build)

    }

    def insertEsBulk(indexName:String,docs:List[Any]): Unit ={
        //Bulk batch
        val jest: JestClient = getClient
        val bulkBuilder = new Bulk.Builder
        bulkBuilder.defaultIndex(indexName).defaultType("_doc")
        for (doc <- docs ) {

            val index: Index = new Index.Builder(doc).build()
            bulkBuilder.addAction(index)
        }

        val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulkBuilder.build()).getItems
        println("保存"+items.size()+"条数据")

        close(jest)

    }


    def main(args: Array[String]): Unit = {
        // 单个document保存
        //     val jest: JestClient = getClient
        //     val movie= "{   \"name\":\"liulangdiqiu\",\n    \"movie_time\": 2.0\n}"
        //     val index: Index = new Index.Builder(movie).index("movie1111_index").`type`("_doc").build()
        //     jest.execute(index)
        //     close(jest)
        //批量保存


        val movieList = List(Movie("dianying1",1.5F) ,Movie("dianying2",1.8F))

        insertEsBulk("movie1111_index",movieList)

    }
}
