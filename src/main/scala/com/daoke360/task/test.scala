package com.daoke360.task

import java.text.SimpleDateFormat
import java.util.{Base64, Locale}

import com.alibaba.fastjson.JSON
import com.daoke360.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lian on 2018/5/3.
  */
object test {
  def parseNginxTime2Long(nginxTime: String) = {
    val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +0800", Locale.ENGLISH)
    val date = sdf.parse(nginxTime)
    date.getTime
  }

  def main(args: Array[String]): Unit = {
//      val m="30/Mar/2018:03:23:09 +0800"
//       val sdf1 = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.US)
//       val time = sdf1.parse(m)
//       val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//       val format = sdf.format(time)

//     val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +0800", Locale.ENGLISH)
//     val date = sdf.parse(m)
//    date.getTime

    val sparkConf = new SparkConf().setAppName("test ").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val eventRdd=sc.textFile("C:\\aaa\\bigdata_access.log-2018033101").filter(_.length>0).filter(line=>line.indexOf("bData")>0).take(20)
    val readWriteRDD=eventRdd.map(line=>{
      val sp = line.split("\\|")
      val ip = sp(0)
      val datetime=sp(3)
//          val formatDateTime = Utils.formatDateTime(datetime,"dd/MMM/yyyy:hh:mm:ss Z","yyyy-MM-dd HH:mm:ss")
//          val parseDate2Long = Utils.parseDate2Long(formatDateTime,"yyyy-MM-dd HH:mm:ss")
      val baseData=sp(4)
          val newBaseData=baseData.split(" ")
          val request = newBaseData(0)
              val base64Data=newBaseData(1)
              val dataString = base64Data.split("=")

                   val stringbig = dataString(0)
                       val stringbigdata = stringbig.split("\\?")
                            val  stringbehavior=stringbigdata(1)

                   val dataString1 = dataString(1)
                   val decodes = Base64.getDecoder.decode(dataString1)
                   val ts = new String(decodes,"utf-8")
                   val parseObject = JSON.parse(ts)


      val bigdata = sp(7)
          val bigdatas = bigdata.split("\\/")
          val bigdatasdalvik = bigdatas(1)
               val splitdata = bigdatasdalvik.split(";")
               val bigdatadavik3 = splitdata(2)
          val bigdatasdalvik2= bigdatas(2)
               val bigdatasdalvik4 = bigdatasdalvik2.split(")")
               val bigdatasdalvik5 = bigdatasdalvik4(0)

//      (ip,parseDate2Long,request,stringbehavior,parseObject,bigdatasdalvik,bigdatasdalvik2)

    })
    /**
      * ip =  219.157.54.132
      * parseDate2Long=2018-03-30
      * request=GET
      * stringbehavior=bData
      * jsonParseObject={"behaviorData":{"channelId":"52","zongKey":"FM206"},"behaviorKey":"DFSJ100","ktingToken":"wmIjj/TN8W41mGvIr9RnJ6C+6F7oqWAcoOWZLen6zTOPANszI7ccURqANHdvNYKVCFq82l0QYRqF905HZj2CVw=="},
      * bigdata="Dalvik/2.1.0 (Linux; U; Android 6.0; HUAWEI VNS-AL00 Build/HUAWEIVNS-AL00)" sendfileon)
      */


    readWriteRDD.foreach(t=>{
      println(t)
    })
  }

}
