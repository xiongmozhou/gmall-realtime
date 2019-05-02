package com.demo.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.util.Date;

@RestController
public class DictController {

    @GetMapping("dict")
    public  String dict(HttpServletResponse response){
        response.addHeader("Last-Modified",new Date().toString());
        return "蓝瘦香菇";
    }
}
