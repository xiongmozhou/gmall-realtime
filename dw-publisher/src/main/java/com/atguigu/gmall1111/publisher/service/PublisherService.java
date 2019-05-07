package com.atguigu.gmall1111.publisher.service;

import com.atguigu.gmall1111.publisher.bean.SaleInfo;

import java.util.Map;

public interface PublisherService {

    /**
     * 统计日活总数
     * @param date
     * @return
     */
    public int getDauTotal(String date );

    /**
     * 统计分时趋势(时活)
     * @param date
     * @return
     */
    public Map getDauHours(String date );

    /**
     * 统计单日订单量及收入
     * @param date
     * @return
     */
    public Double getOrderTotalAmount(String date );

    /**
     * 统计分时趋势--> 订单量及收入
     * @param date
     * @return
     */
    public Map getOrderTotalAmountHour(String date );

    public SaleInfo getSaleInfo(String date, String keyword, int startPage, int pagesize, String aggsFieldName, int aggsize);

}