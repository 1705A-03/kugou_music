package com.daoke360.task.utils



import com.daoke360.caseclass.{IPRule, RegionInfo}
import scala.util.control.Breaks._
/**
  * Created by Luo on 2018/5/3.
  */
object IPAnalysisUtils {




  /**
    * 查找ip对应的地址信息
    */
  def analysisIP(ip: String, ipRules: Array[IPRule]) = {
    val regionInfo = RegionInfo()
    //将ip转换成完整的十进制
    val numIP = ip2Long(ip)
    //通过二分查找法，查找出ip对应的地区索引
    val index = binnerySearch(numIP, ipRules)
    if (index != -1) {
      val iPRule = ipRules(index)
      regionInfo.country = iPRule.country
      regionInfo.province = iPRule.province
      regionInfo.city = iPRule.city
    }
    regionInfo
  }

  //将ip转换成完整的十进制
  def ip2Long(ip: String) = {
    var numIP: Long = 0
    //Array(1,0,1,0)
    val fileds = ip.split("[.]")
    for (i <- 0 until (fileds.length)) {
      numIP = numIP << 8 | fileds(i).toLong
    }
    numIP
  }


  //通过二分查找法，查找出ip对应的地区索引
  private def binnerySearch(numIP: Long, ipRules: Array[IPRule]) = {
    var min = 0
    var max = ipRules.length - 1
    var index = -1
    breakable({
      while (min <= max) {
        var middle = (min + max) / 2
        val ipRule = ipRules(middle)
        if (numIP >= ipRule.startIP && numIP <= ipRule.endIP) {
          index = middle
          break()
        } else if (numIP < ipRule.startIP) {
          max = middle - 1
        } else if (numIP > ipRule.endIP) {
          min = middle + 1
        }
      }
    })
    index
  }

}
