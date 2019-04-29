package com.atguigu.gmall.dw.realtime.bean

case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      logType:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var logHourMinute:String,
                      var ts:Long
                     ){}
