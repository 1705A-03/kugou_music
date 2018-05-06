package com.daoke360.task.analysislog


import com.daoke360.caseclass.IPRule
import com.daoke360.common.{EventLogContants, GlobalContants}
import com.daoke360.task.utils.LogAnalysisUtils
import com.daoke360.utils.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
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
    val jobConf = new JobConf(new Configuration())
    //指定输出的类，这个类专门用来将spark处理的结果写入到hbase中
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定要将数据写入到哪张表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, EventLogContants.HBASE_EVENT_LOG_TABLE)
    //验证输入参数是否正确
    processArgs(args,sparkConf)
    //处理输入路径
    processInputPath(sparkConf)


    val sc = new SparkContext(sparkConf)

    //加载ip规则库
    val ipRules: Array[IPRule] = sc.textFile("/spark_data/ip.data").map(line => {
      val fields = line.split("\\|")
      IPRule(fields(2).toLong, fields(3).toLong, fields(5), fields(6), fields(7))
    }).collect()

    val ipRulesBroadCast = sc.broadcast(ipRules)

    /**
      * 加载hdfs的日志
      */
//        val filterRDD = sc.textFile("C:\\aaa\\bigdata_access.log-2018033101").filter(_.length>0).filter(line=>line.indexOf("bData")>0)
    val filterRDD = sc.textFile(sparkConf.get(GlobalContants.TASK_INPUT_PATH)).filter(_.length>0).filter(line=>line.indexOf("bData")>0)

     val eventLogMap = filterRDD.map(logText=>{
       LogAnalysisUtils.analysisLog(logText,ipRulesBroadCast.value)
    })

    val tuple2RDD = eventLogMap.map(map => {
      /**
        * 构建rowkey原则：
        * 1，唯一性 2，散列 3，长度不能过长，4，方便查询
        *
        * acceipss_time+"_"+ ip
        */
      //用户访问时间
      val accessTime = map(EventLogContants.LOG_COLUMN_NAME_ACCESS_TIME)
      //用户ip
      val ip = map(EventLogContants.LOG_COLUMN_NAME_IP)
      //构建rowkey
      val rowKey = accessTime + "_" + Math.abs(ip.hashCode)
      //构建put对象ip
      val put = new Put(rowKey.getBytes())
      map.foreach(t2 => {
        put.addColumn(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), t2._1.getBytes(), t2._2.getBytes())
      })
      //保存到hbase中的数据一定要是对偶元组格式的
      (new ImmutableBytesWritable(), put)
    })
    tuple2RDD.saveAsHadoopDataset(jobConf)
    sc.stop()
  }
}
