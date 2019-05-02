package com.atguigu.gmall1111.publisher.service.impl;

import com.atguigu.gmall.dw.realtime.constant.GmallConstant;
import com.atguigu.gmall1111.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.atguigu.gmall.dw.realtime.constant.GmallConstant.ES_TYPE_DAU;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    JestClient jestClient;

    /**
     * 从es中查询当日日活总数
     * @param date
     * @return
     */
    @Override
    public int getDauTotal(String date) {
        String query = "{\n" +
                "  \"query\" : {\n" +
                "    \"bool\" : {\n" +
                "      \"filter\" : {\n" +
                "        \"match\" : {\n" +
                "          \"logDate\" : {\n" +
                "            \"query\" : \""+date+"\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Search search = new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_TYPE_DAU).build();
        int total = 0;
        try {
            SearchResult res = jestClient.execute(search);
            total = res.getTotal();
            System.out.println(total);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }


    /**
     * 日活分时统计
     * @param date
     * @return
     */
    @Override
    public  Map getDauHours(String date) {
        String query = " {\n" +
                "  \"query\" : {\n" +
                "    \"bool\" : {\n" +
                "      \"filter\" : {\n" +
                "        \"match\" : {\n" +
                "          \"logDate\" : {\n" +
                "            \"query\" : \""+date+"\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupby_hour\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"logHour\" \n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Search search = new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_TYPE_DAU).build();
        //new 一个map然后保存数据
        HashMap<String, Long> map = new HashMap<>();
        try {
            SearchResult result = jestClient.execute(search);
            //得到结果，然后过滤出想要的结果
            TermsAggregation groupby_hour = result.getAggregations().getTermsAggregation("groupby_hour");
            List<TermsAggregation.Entry> buckets = groupby_hour.getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                map.put(bucket.getKey(),bucket.getCount());
//                System.out.println(bucket.getKey()+":"+bucket.getCount());
            }

        } catch (IOException  e) {
            e.printStackTrace();
        }

        return map;
    }

}
