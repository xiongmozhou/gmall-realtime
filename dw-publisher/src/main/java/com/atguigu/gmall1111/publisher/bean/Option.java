package com.atguigu.gmall1111.publisher.bean;


/**
 * 某个选项
 */
public class Option {

    String name;

    Double value;

    public Option(String name, Double value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
}
