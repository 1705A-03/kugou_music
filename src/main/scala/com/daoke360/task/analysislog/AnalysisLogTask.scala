package com.daoke360.task.analysislog


import com.daoke360.caseclass.IPRule
import com.daoke360.common.GlobalContants
import com.daoke360.task.utils.LogAnalysisUtils
import com.daoke360.utils.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext, SparkException}


/**
  * Created by lian on 2018/5/3.
  */
object AnalysisLogTask {
  //验证输入参数是否正确
  //TASK_PARAMS_FLAG: String = "-d"   TASK_RUN_DATE = "task_run_date"
  def processArgs(args: Array[String], sparkConf: SparkConf) = {
    if(args.length>=2 && args(0).equals(GlobalContants.TASK_PARAMS_FLAG)&&Utils.validateInpuDate(args(1))){
      sparkConf.set(GlobalContants.TASK_RUN_DATE,args(1))
    }else{
      throw new SparkException(
        """
          |At least two parameters are required for the task example: xx.jar -d yyyy-MM-dd
          |<-d>:Marking of the start of task parameters
          |<yyyy-MM-dd>:Task run date
        """.stripMargin
      )
    }
  }

  //处理输入路径
  //TASK_RUN_DATE = "task_run_date"   LOG_DIR_PREFIX = "/logs/"  TASK_INPUT_PATH: String = "task_input_path"
  def processInputPath(sparkConf: SparkConf) = {

    var fs: FileSystem = null
    try {
      //将字符串日期转换成long类型的时间戳
      val longTime = Utils.parseDate2Long(sparkConf.get(GlobalContants.TASK_RUN_DATE), "yyyy-MM-dd")
      //将时间戳转换成指定格式的日期
      val inputDate = Utils.formatDate(longTime, "yyyy/MM/dd")
      val inputPath = new Path(GlobalContants.LOG_DIR_PREFIX + inputDate)
      fs = FileSystem.newInstance(new Configuration())
      if (fs.exists(inputPath)) {
        sparkConf.set(GlobalContants.TASK_INPUT_PATH, inputPath.toString)
      } else {
        throw new Exception("not found input path of task....")
      }
    } catch {
      case e: Exception => throw e
    } finally {
      if (fs != null) {
        fs.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    //验证输入参数是否正确
    processArgs(args,sparkConf)
    //处理输入路径
    processInputPath(sparkConf)


    val sc = new SparkContext(sparkConf)

    //加载ip规则库
    val ipRules: Array[IPRule] = sc.textFile("/spark_sf_project/resource/ip.data").map(line => {
      val fields = line.split("\\|")
      IPRule(fields(2).toLong, fields(3).toLong, fields(5), fields(6), fields(7))
    }).collect()

    val ipRulesBroadCast = sc.broadcast(ipRules)

    /**
      * 加载hdfs的日志
      */
//        val filterRDD = sc.textFile("C:\\aaa\\bigdata_access.log-2018033101").filter(_.length>0).filter(line=>line.indexOf("bData")>0)
    val filterRDD = sc.textFile(sparkConf.get(GlobalContants.TASK_INPUT_PATH)).filter(_.length>0).filter(line=>line.indexOf("bData")>0).take(3000)

    filterRDD.map(logText=>{
       LogAnalysisUtils.analysisLog(logText,ipRulesBroadCast.value)
    }).foreach(println(_))
//    Map(os_n -> 6.0, ip -> 219.157.54.132, city -> 洛阳, behaviorKey -> DFSJ100, access_time -> 1522351389000, country -> 中国, os_v -> Android, province -> 河南, modelNum -> HUAWEIVNS-AL00, request_type -> GET , behaviorData -> {"channelId":"52","zongKey":"FM206"}, behavior -> bData)
//    Map(os_n -> 8.0.0, ip -> 183.227.7.117, city -> 重庆, behaviorKey -> DFSJ101, access_time -> 1522351389000, country -> 中国, os_v -> Android, province -> 重庆, modelNum -> HUAWEIHWI-AL00, request_type -> GET , behaviorData -> {"zongKey":"FM100"}, behavior -> bData)
//    Map(os_n -> 8.0.0, ip -> 222.85.235.216, city -> 贵阳, behaviorKey -> DFSJ103, access_time -> 1522351389000, country -> 中国, os_v -> Android, province -> 贵州, modelNum -> HUAWEIBKL-AL00, request_type -> GET , behaviorData -> {"zongKey":"FM601"}, behavior -> bData)
//    Map(os_n -> 6.0, ip -> 219.157.54.132, city -> 洛阳, behaviorKey -> DFSJ100, access_time -> 1522351389000, country -> 中国, os_v -> Android, province -> 河南, modelNum -> HUAWEIVNS-AL00, request_type -> GET , behaviorData -> {"channelId":"59","zongKey":"FM206"}, behavior -> bData)


    sc.stop()
  }
}
