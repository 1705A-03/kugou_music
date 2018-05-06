package com.daoke360.task.utils

import java.util.Base64

import com.alibaba.fastjson.JSON
import com.daoke360.caseclass.IPRule
import com.daoke360.common.{EventLogContants, GlobalContants}
import com.daoke360.utils.Utils
import org.apache.commons.lang.StringUtils

import scala.collection.mutable

/**
  * Created by Luo on 2018/5/3.
  */
object LogAnalysisUtils {

  /**
    * //处理请求参数
    * GET
    * /?bData=eyJrdGluZ1Rva2VuIjoid21JampcL1ROOFc0MW1HdklyOVJuSjZDKzZGN29xV0Fjb09XWkxlbjZ6VE9QQU5zekk3Y2NVUnFBTkhkdk5ZS1ZDRnE4MmwwUVlScUY5MDVIWmoyQ1Z3PT0iLCJiZWhhdmlvcktleSI6IkRGU0oxMDAiLCJiZWhhdmlvckRhdGEiOnsiem9uZ0tleSI6IkZNMjA2IiwiY2hhbm5lbElkIjoiNTIifX0= HTTP
    * /1.1
    * {"behaviorData":{"programId":"325518","zongKey":"FM401"},"behaviorKey":"DFSJ200","ktingToken":"7belXVEzULn84wDXmc+BetvkKaqqJQaXZu4szgcIDFisijb9NzIUXAQ9piR7Off/BdDl4igt26PaDuP+NrPXjg=="}
    * @param requestBody
    * @param everntLogMap
    */
  def handlerRequestBody(requestBody: String, everntLogMap: mutable.HashMap[String, String]) = {

    val fields = requestBody.split("\\/")
    if(fields.length == 3){
      everntLogMap.put(EventLogContants.LOG_COLUMN_REQUEST_TYPE,fields(0))
      //  /?bData=eyJrdGluZ1Rva2VuIjoid21JampcL1ROOFc0MW1HdklyOVJuSjZDKzZGN29xV0Fjb09XWkxlbjZ6VE9QQU5zekk3Y2NVUnFBTkhkdk5ZS1ZDRnE4MmwwUVlScUY5MDVIWmoyQ1Z3PT0iLC
      val requestParams= fields(1).split(" ")
      if(requestParams.length==2){
        val dataParams = requestParams(0).split("\\=")
        //?pData
        val datatype = dataParams(0).split("\\?")
        if(datatype.length==2){
          everntLogMap.put(EventLogContants.LOG_COLUMN_NAME_BEHAVIOR,datatype(1))
        }
        if(dataParams.length==2){
          val decoders = Base64.getDecoder.decode(dataParams(1))
          val str = new String(decoders,"utf-8")
          var parseObject = JSON.parseObject(str)
          val behaviordata:String = parseObject.getString("behaviorData")
          checkOutBHD(behaviordata,everntLogMap)

          val behaviorkey:String = parseObject.getString("behaviorKey")

          if(behaviordata != null){
            everntLogMap.put(EventLogContants.LOG_COLUMN_NAME_BEHAVIORDATA,behaviordata.toString)
            if(behaviorkey != null){
              everntLogMap.put(EventLogContants.LOG_COLUMN_NAME_BEHAVIORKEY,behaviorkey.toString)
            }
          }
        }
      }
    }

  }

  /**
    *
    * @param bhd 穿进去的jason串
    * @param eventLogMap 传出来的map
    * @return
    */
  def checkOutBHD(bhd: String, eventLogMap: mutable.HashMap[String, String]) = {
    val bhdMes = "["+bhd+"]"
    val arr = JSON.parseArray(bhdMes)
    for(i <- 0 until(arr.size())){
      val jsonStr = arr.getJSONObject(i)//这里就是json数据：{"albumId":"16287","anchorId":"11572","on-off":false,"playTime":0,"programId":"314670","zongKey":"FM702"}
      val keySet = jsonStr.keySet()//数据中所有的key  ：albumId  anchorId  off  playTime  programId  zongKey
      val key = keySet.iterator()
      while (key.hasNext){
        val nextKey = key.next()  //key  :albumId
        val value = jsonStr.get(nextKey)//value ：16287
        eventLogMap.put(nextKey,value.toString)
      }
    }
    eventLogMap
  }

  //处理ip地址
  def handlerIp(eventLogMap:mutable.HashMap[String,String],ipRules:Array[IPRule]) ={

    val ip = eventLogMap(EventLogContants.LOG_COLUMN_NAME_IP)
    val regionInfo=IPAnalysisUtils.analysisIP(ip,ipRules)
    eventLogMap.put(EventLogContants.LOG_COLUNMN_NAME_COUNTRY,regionInfo.country)
    eventLogMap.put(EventLogContants.LOG_COLUMN_NAME_PROVINCE,regionInfo.province)
    eventLogMap.put(EventLogContants.LOG_COLUMN_NAME_CITY,regionInfo.city)
  }



  //处理http   "Dalvik/2.1.0 (Linux; U; Android 6.0; HUAWEI VNS-AL00 Build/HUAWEIVNS-AL00)" sendfileon
  def handlerHttpBody(httpBody: String, everntLogMap: mutable.HashMap[String, String]) = {
    val fields = httpBody.split("\\/")
    if(fields.length==3){
      val version = fields(1).split("\\;")
      val system = fields(2).split("\\)")
      everntLogMap.put(EventLogContants.LOG_COLUMN_NAME_MODELNUM,system(0))
       if(version.length==4){
         val svion = version(2).split(" ")
              everntLogMap.put(EventLogContants.LOG_COLUMN_NAME_OS_N,svion(1))
              everntLogMap.put(EventLogContants.LOG_COLUMN_NAME_OS_V,svion(2))

       }

    }
  }

  /**
    * 解析单挑日志
    * 219.157.54.132|0.000|-|30/Mar/2018:03:23:09 +0800|GET /?bData=eyJrdGluZ1Rva2VuIjoid21JampcL1ROOFc0MW1HdklyOVJuSjZDKzZGN29xV0Fjb09XWkxlbjZ6VE9QQU5zekk3Y2NVUnFBTkhkdk5ZS1ZDRnE4MmwwUVlScUY5MDVIWmoyQ1Z3PT0iLCJiZWhhdmlvcktleSI6IkRGU0oxMDAiLCJiZWhhdmlvckRhdGEiOnsiem9uZ0tleSI6IkZNMjA2IiwiY2hhbm5lbElkIjoiNTIifX0= HTTP/1.1
    * |200|5|"Dalvik/2.1.0 (Linux; U; Android 6.0; HUAWEI VNS-AL00 Build/HUAWEIVNS-AL00)" sendfileon
    */
  def analysisLog(logText:String,ipRules:Array[IPRule])={
   var everntLogMap:mutable.HashMap[String,String]=null
    if(StringUtils.isNotBlank(logText)){
        val fields= logText.split(GlobalContants.LOG_SPLIT_FLAG)
      if(fields.length == 8){
       everntLogMap = new mutable.HashMap[String,String]()
        //用户ip
        everntLogMap.put(EventLogContants.LOG_COLUMN_NAME_IP,fields(0))
        //处理ip
        handlerIp(everntLogMap,ipRules)
        //用户访问时间  30/Mar/2018:03:24:01 +0800 转换成yyyy-MM-dd HH:mm:ss 再转换成Long类型
        var access=Utils.formatDateTime(fields(3),"dd/MMM/yyyy:hh:mm:ss Z","yyyy-MM-dd HH:mm:ss")
        var access_time=Utils.parseDate2Long(access,"yyyy-MM-dd HH:mm:ss").toString
        everntLogMap.put(EventLogContants.LOG_COLUMN_NAME_ACCESS_TIME,access_time)

        //请求
        val requestBody = fields(4)
        //处理请求参数
        handlerRequestBody(requestBody,everntLogMap)

        val httpBody=fields(7)
        //处理http
        handlerHttpBody(httpBody,everntLogMap)
      }
    }
    everntLogMap
  }
}
