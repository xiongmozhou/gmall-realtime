package com.atguigu.gmall1111.publisher.bean;

import java.util.List;
import java.util.Map;


/**
 * 整个统计结果
 */
public class SaleInfo {

    Integer total ;

    List<OptionGroup> stat;

    List<Map>  detail;


    public Map getTempAggsMap() {
        return tempAggsMap;
    }

    public void setTempAggsMap(Map tempAggsMap) {
        this.tempAggsMap = tempAggsMap;
    }

    Map tempAggsMap;


    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public List<OptionGroup> getStat() {
        return stat;
    }

    public void setStat(List<OptionGroup> stat) {
        this.stat = stat;
    }

    public List<Map> getDetail() {
        return detail;
    }

    public void setDetail(List<Map> detail) {
        this.detail = detail;
    }
}
