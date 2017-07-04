package com.lakala.finance.stream.screen.recov

import com.alibaba.druid.pool.DruidDataSource
import com.lakala.finance.common.EnvUtil
import com.lakala.finance.stream.screen.biz.BizSql
import com.lakala.finance.stream.screen.common.DateTimeUtils
import org.apache.spark.Logging
import redis.clients.jedis.JedisCluster
import com.lakala.finance.stream.screen.biz.Constant

import scala.collection.mutable.ArrayBuffer

/**
  * Created by longxiaolei on 2017/3/30.
  */
object RecovData extends Logging {
  //  private val jedis: JedisCluster = RedisUtils.jedisCluster()

  def recovFund(mysqlDS: DruidDataSource, jedis: JedisCluster, startTimeStr: String, endTimeStr: String, end_suffix: String) = {
    val query_fund_info: ArrayBuffer[(Long, Long, String)] = BizSql.query_fund_info(mysqlDS, "fund", startTimeStr, endTimeStr)
    val query_finacing_info: ArrayBuffer[(Long, Long, String)] = BizSql.query_fund_info(mysqlDS, "finacing", startTimeStr, endTimeStr)

    query_fund_info.foreach { case (count, amount, dayStr) => {
      println("基金 " + dayStr + " " + count + "," + amount)
      val countKye: String = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FUND_COUNT + end_suffix
      val amountKey = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FUND_AMOUNT + end_suffix
      val total_countKey: String = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FUND_FINANCING_TOTAL_COUNT + end_suffix
      val total_amountKey: String = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FUND_FINANCING_TOTAL_AMOUNT + end_suffix
      jedis.incrBy(countKye, count)
      jedis.incrBy(amountKey, amount)
      jedis.incrBy(total_countKey, count)
      jedis.incrBy(total_amountKey, amount)
      jedis.expire(countKye, Constant.Redis.EXPIRE)
      jedis.expire(amountKey, Constant.Redis.EXPIRE)
      jedis.expire(amountKey, Constant.Redis.EXPIRE)
      jedis.expire(total_amountKey, Constant.Redis.EXPIRE)
    }
    }

    query_finacing_info.foreach { case (count, amount, dayStr) => {
      println("理财 " + dayStr + " " + count + "," + amount)
      val countKye: String = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FINANCING_COUNT + end_suffix
      val amountKey = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FINANCING_AMOUNT + end_suffix
      val total_countKey: String = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FUND_FINANCING_TOTAL_COUNT + end_suffix
      val total_amountKey: String = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FUND_FINANCING_TOTAL_AMOUNT + end_suffix
      jedis.incrBy(countKye, count)
      jedis.incrBy(amountKey, amount)
      jedis.incrBy(total_countKey, count)
      jedis.incrBy(total_amountKey, amount)
      jedis.expire(countKye, Constant.Redis.EXPIRE)
      jedis.expire(amountKey, Constant.Redis.EXPIRE)
      jedis.expire(amountKey, Constant.Redis.EXPIRE)
      jedis.expire(total_amountKey, Constant.Redis.EXPIRE)
    }
    }

  }


  def recovDaily(mysqlDS: DruidDataSource, jedis: JedisCluster, startTimeStr: String, endTimeStr: String, end_suffix: String) = {
    //(amount,collectIme,province,city)
    val apply_inf: ArrayBuffer[(Long, String, String, String)] = BizSql.query_apply_or_loan_info(mysqlDS, "apply", startTimeStr, endTimeStr)
    val loan_inf: ArrayBuffer[(Long, String, String, String)] = BizSql.query_apply_or_loan_info(mysqlDS, "loan", startTimeStr, endTimeStr)

    println("进件总数 :" + apply_inf.size)
    println("审批总数 :" + loan_inf.size)

    if (apply_inf.size > 0) {
      val groupedApplyInfo: Map[String, (Long, Int, String)] = apply_inf.map(x => (x._1, 1, x._2.substring(0, 10))) //将数据打成（amount,1,dayStr）
        .groupBy(_._3) //根据每天的日期进行分组
        .mapValues(_.reduce((a, b) => {
        //对分组后的数据进行汇总
        (a._1 + b._1, a._2 + b._2, a._3)
      }))
      groupedApplyInfo.foreach(x => {
        //将每天的信息刷到redis
        val dayStr: String = x._1
        val amount: Long = x._2._1
        val count: Int = x._2._2
        val daily_apply_count: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + dayStr + "." + Constant.Redis.DAILY_COUNT_SUFFIX + end_suffix
        val daily_apply_amount: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + dayStr + "." + Constant.Redis.DAILY_AMOUNT_SUFFIX + end_suffix
        println("恢复进件数量:" + dayStr + "," + amount + "," + count)
        jedis.incrBy(daily_apply_count, count)
        jedis.incrBy(daily_apply_amount, amount)
        jedis.expire(daily_apply_count, Constant.Redis.EXPIRE)
        jedis.expire(daily_apply_amount, Constant.Redis.EXPIRE)
      })
    }

    if (loan_inf.size > 0) {
      val groupedLoanInfo: Map[String, (Long, Int, String)] = loan_inf.map(x => (x._1, 1, x._2.substring(0, 10))) //将数据打成（amount,1,dayStr）
        .groupBy(_._3) //根据每天的日期进行分组
        .mapValues(_.reduce((a, b) => {
        //对分组后的数据进行汇总
        (a._1 + b._1, a._2 + b._2, a._3)
      }))
      groupedLoanInfo.foreach(x => {
        //将每天的信息刷到redis
        val dayStr: String = x._1
        val amount: Long = x._2._1
        val count: Int = x._2._2
        val daily_apply_count: String = Constant.Redis.PREFIX + Constant.LogNo.LOAN_FLAG + "." + dayStr + "." + Constant.Redis.DAILY_COUNT_SUFFIX + end_suffix
        val daily_apply_amount: String = Constant.Redis.PREFIX + Constant.LogNo.LOAN_FLAG + "." + dayStr + "." + Constant.Redis.DAILY_AMOUNT_SUFFIX + end_suffix
        println("恢复审计数量:" + dayStr + "," + amount + "," + count)
        jedis.incrBy(daily_apply_count, count)
        jedis.incrBy(daily_apply_amount, amount)
        jedis.expire(daily_apply_count, Constant.Redis.EXPIRE)
        jedis.expire(daily_apply_amount, Constant.Redis.EXPIRE)
      })
    }
  }

  /**
    * 计算输入时间段内的地图信息，并将之对应的值刷入到redis中，没有清理老数据的逻辑
    *
    * @param jedis
    * @param startTimeStr
    * @param endTimeStr
    */
  def recovSub(mysqlDS: DruidDataSource, jedis: JedisCluster, startTimeStr: String, endTimeStr: String, end_suffix: String) = {
    //(amount,collectIme,province,city)
    val apply_inf: ArrayBuffer[(Long, String, String, String)] = BizSql.query_apply_or_loan_info(mysqlDS, "apply", startTimeStr, endTimeStr)
    val loan_inf: ArrayBuffer[(Long, String, String, String)] = BizSql.query_apply_or_loan_info(mysqlDS, "loan", startTimeStr, endTimeStr)

    if (apply_inf.size > 0) {
      val groupedApply_info: Map[(Long, String), ArrayBuffer[(Long, Int, Long, String)]] = apply_inf.map(x => {
        val amount: Long = x._1
        val collectTime: String = x._2
        val timeStamp: Long = DateTimeUtils.getTimeStampOfStr(collectTime)
        val sub_timeStamp: Long = DateTimeUtils.getIntervalTimeStr(timeStamp)
        (amount, 1, sub_timeStamp, collectTime.substring(0, 10))
      }).groupBy(x => (x._3, x._4)) // (amount,1,subTimeStam,dayStr) 根据subTimeStam,dayStr 组合分组
      val reducedValues: Map[(Long, String), (Long, Int, Long, String)] = groupedApply_info.mapValues(_.reduce((a, b) => {
          (a._1 + b._1, a._2 + b._2, a._3, b._4)
        }))
      reducedValues.foreach(x => {
        val (amount, count, subTime, dayStr) = x._2
        val count_key: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + dayStr + "." + Constant.Redis.SUBSECTION_COUNT_SUFFIX + end_suffix
        val amount_key: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + dayStr + "." + Constant.Redis.SUBSECTION_AMOUNT_SUFFIX + end_suffix
        println("补数的时间段数据_进件：" + dayStr + "," + subTime + "," + amount + "," + count + "," + count_key + "," + amount_key)
        jedis.hincrBy(count_key, subTime.toString, count)
        jedis.hincrBy(amount_key, subTime.toString, amount)
        jedis.expire(count_key, Constant.Redis.EXPIRE)
        jedis.expire(amount_key, Constant.Redis.EXPIRE)
      })
    }

    if (loan_inf.size > 0) {
      val groupedLoan_info: Map[(Long, String), ArrayBuffer[(Long, Int, Long, String)]] = loan_inf.map(x => {
        val amount: Long = x._1
        val collectTime: String = x._2
        val timeStamp: Long = DateTimeUtils.getTimeStampOfStr(collectTime)
        val sub_timeStamp: Long = DateTimeUtils.getIntervalTimeStr(timeStamp)
        (amount, 1, sub_timeStamp, collectTime.substring(0, 10))
      }).groupBy(x => (x._3, x._4)) // (amount,1,subTimeStam,dayStr) 根据subTimeStam,dayStr 组合分组
      val reducedValues: Map[(Long, String), (Long, Int, Long, String)] = groupedLoan_info.mapValues(_.reduce((a, b) => {
          (a._1 + b._1, a._2 + b._2, a._3, b._4)
        }))
      reducedValues.foreach(x => {
        val (amount, count, subTime, dayStr) = x._2
        val count_key: String = Constant.Redis.PREFIX + Constant.LogNo.LOAN_FLAG + "." + dayStr + "." + Constant.Redis.SUBSECTION_COUNT_SUFFIX + end_suffix
        val amount_key: String = Constant.Redis.PREFIX + Constant.LogNo.LOAN_FLAG + "." + dayStr + "." + Constant.Redis.SUBSECTION_AMOUNT_SUFFIX + end_suffix
        println("补数的时间段数据_审计：" + dayStr + "," + subTime + "," + amount + "," + count + "," + count_key + "," + amount_key)
        jedis.hincrBy(count_key, subTime.toString, count)
        jedis.hincrBy(amount_key, subTime.toString, amount)
        jedis.expire(count_key, Constant.Redis.EXPIRE)
        jedis.expire(amount_key, Constant.Redis.EXPIRE)
      })
    }

  }

  /**
    * 根据开始和结束时间恢复计算出地图信息，然后跟redis中的值进行累加
    *
    * @param jedis
    * @param startTimeStr
    * @param endTimeStr
    */
  def recovLocation(mysqlDS: DruidDataSource, jedis: JedisCluster, startTimeStr: String, endTimeStr: String, end_suffix: String) = {
    //(amount,collectIme,province,city)
    val apply_inf: ArrayBuffer[(Long, String, String, String)] = BizSql.query_apply_or_loan_info(mysqlDS, "apply", startTimeStr, endTimeStr)

    if (apply_inf.size > 0) {
      //计算省份的信息
      apply_inf.map(x => {
        val (amount, collectIme, province, city) = x
        (1, province, collectIme.substring(0, 10))
      }).groupBy(x => (x._2, x._3)).mapValues(_.reduce((a, b) => {
        (a._1 + b._1, a._2, a._3)
      })).foreach(x => {
        val (count, province, dayStr) = x._2
        val hprovinceKey: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + dayStr + "." + Constant.Redis.PROVINCE_DAILY_COUNT + end_suffix
        println("省份的数据_进件数量：" + province + "," + count)
        jedis.hincrBy(hprovinceKey, province, count)
        jedis.expire(hprovinceKey, Constant.Redis.EXPIRE)
      })

      //计算城市的总数
      apply_inf.map(x => {
        val (amount, collectIme, province, city) = x
        (1, city, collectIme.substring(0, 10))
      }).groupBy(x => (x._2, x._3)).mapValues(_.reduce((a, b) => {
        (a._1 + b._1, a._2, a._3)
      })).foreach(x => {
        val (count, province, dayStr) = x._2
        val hcityKey: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + dayStr + "." + Constant.Redis.CITY_DAILY_COUNT + end_suffix
        println("城市的数据_进件数量：" + province + "," + count + "," + hcityKey)
        jedis.hincrBy(hcityKey, province, count)
        jedis.expire(hcityKey, Constant.Redis.EXPIRE)
      })
    }


  }

  def asserArgs(args: Array[String]): Boolean = {
    if (!(args.length == 1 || args.length == 3)) {
      println(
        s"""   Usage: <env_flag> <recovery_flag> <startTime> <endTime>
            |  <env_flag> 标识环境种类
            |  <recovery_flag> 恢复类型，现阶段只有三种：daily->标识恢复每日总计 location->恢复地图数据 sub->恢复5分钟的曲线信息
            |  <startTime> 恢复起始时间
            |  <endTime> 恢复结束时间
            |  注意：startTime endTime 为组合输入，要么都输入，要么都不输入;都不输入则为根据水位记录的时间恢复当天的数据，
            |  输入的时间格式为：yyyy-MM-dd HH:mm:ss
        """.stripMargin)
      return true
    }
    false
  }


  def main(args: Array[String]) {
    if (asserArgs(args)) return
    val env: String = args(0)
    //判断是否是生产的测试环境，因为生产的测试环境和真实的生产环境对应的环境信息相同，而对应的rediskey值不一样
    val end_suffix: String = if (env.endsWith("_test")) "_test" else ""

    var startTimeStr: String = null
    var endTimeStr: String = null
    val jedis: JedisCluster = EnvUtil.jedisCluster(env)
    val mysqlDS: DruidDataSource = EnvUtil.mysqlDS(env)

    var water_point: String = "dataPlatform.water_pointer" + end_suffix
    if (args.length == 3) {
      //根据入参的情况来设定开始和结束时间
      val pattern = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"
      val r1: Boolean = args(2).matches(pattern)
      val r2: Boolean = args(3).matches(pattern)
      if (r1 && r2) {
        startTimeStr = args(2)
        endTimeStr = args(3)
      } else {
        println("请按照 yyyy-MM-dd HH:mm:ss 的格式输入时间")
        return
      }
    } else {
      //如果没有输入参数则根据水位来计算开始和结束时间
      import scala.collection.JavaConversions._
      val smembers: scala.collection.mutable.Set[String] = jedis.smembers(water_point)

      if (smembers == null || smembers.size == 0) {
        //如果水位的值为空
        println(s"恢复水位为空，请先等待streaming程序处理水位的值")
        return
      }

      val timeStamps: Array[Long] = smembers.map(_.toLong).toArray
      val reverse: Array[Long] = timeStamps.sorted
      val endTime: Long = reverse(0)

      endTimeStr = DateTimeUtils.getSecondTimStr(endTime)
      //计算开始时间为水位标记的前一天
      startTimeStr = DateTimeUtils.getYesterdayStr(endTimeStr.substring(0, 10))

      println("计算开始时间" + startTimeStr)
      println("计算结束时间" + endTimeStr)
    }

    recovDaily(mysqlDS, jedis, startTimeStr, endTimeStr, end_suffix)
    recovLocation(mysqlDS, jedis, startTimeStr, endTimeStr, end_suffix)
    recovSub(mysqlDS, jedis, startTimeStr, endTimeStr, end_suffix)
    recovFund(mysqlDS, jedis, startTimeStr, endTimeStr, end_suffix)

    jedis.del(water_point)
  }

}
