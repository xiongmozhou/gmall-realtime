package com.atguigu.gmall1111.publisher.service.impl;

import com.atguigu.gmall.dw.realtime.constant.GmallConstant;
import com.atguigu.gmall1111.publisher.bean.SaleInfo;
import com.atguigu.gmall1111.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    /**
     * 单日订单量及收入
     * @param date
     * @return
     */
    @Override
    public Double getOrderTotalAmount(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        SumBuilder aggsSum = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        searchSourceBuilder.aggregation(aggsSum);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType(GmallConstant.ES_TYPE_DAU).build();

        Double sumTotalAmount =0D;
        try {
            SearchResult searchResult = jestClient.execute(search);
            sumTotalAmount = searchResult.getAggregations().getSumAggregation("sum_totalamount").getSum();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sumTotalAmount;
    }

    /**
     * 分时统计订单量及收入
     * @param date
     * @return
     */
    @Override
    public Map getOrderTotalAmountHour(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合  groupby
        TermsBuilder termsAggs = AggregationBuilders.terms("groupby_createHour").field("createHour").size(24);
        //子聚合 sum
        SumBuilder sumAggs = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        termsAggs.subAggregation(sumAggs);

        searchSourceBuilder.aggregation(termsAggs);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType(GmallConstant.ES_TYPE_DAU).build();

        Map totalAmountHourMap=new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                //小时
                String hourkey = bucket.getKey();
                //小时的金额
                Double totalAmountHour = bucket.getSumAggregation("sum_totalamount").getSum();
                totalAmountHourMap.put(hourkey, totalAmountHour)  ;
            }

        }catch (IOException e) {
            e.printStackTrace();
        }
        return totalAmountHourMap;
    }


    @Override
    public SaleInfo getSaleInfo(String date, String keyword, int startPage, int pagesize, String aggsFieldName, int aggsize) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //过滤：日期
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        //匹配： 商品关键词
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);
        //聚合
        TermsBuilder termAggs = AggregationBuilders.terms("groupby_" + aggsFieldName).field(aggsFieldName).size(aggsize);
        searchSourceBuilder.aggregation(termAggs);
        //分页
        searchSourceBuilder.from((startPage-1)*pagesize);
        searchSourceBuilder.size(pagesize);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE).addType(GmallConstant.ES_TYPE_DAU).build();


        SaleInfo saleInfo = new SaleInfo();
        List<Map> detailList = new ArrayList();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //总数
            saleInfo.setTotal(searchResult.getTotal());
            //明细
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                Map source = hit.source;
                detailList.add(source);
            }
            saleInfo.setDetail(detailList);
            //饼图（聚合结果）
            Map aggsTempMap=new HashMap();
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_" + aggsFieldName).getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                aggsTempMap.put(bucket.getKey(),bucket.getCount());
            }
            saleInfo.setTempAggsMap(aggsTempMap);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return saleInfo;
    }
}
