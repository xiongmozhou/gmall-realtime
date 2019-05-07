package com.atguigu.gmall1111.publisher.bean;

import java.util.List;

/**
 * 某个饼图
 */
public class OptionGroup {

    public OptionGroup(List<Option> options, String title) {
        this.options = options;
        this.title = title;
    }

    List<Option>  options ;

    String title;

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
