package com.lakala.finance.stream.screen.biz

import java.lang.Long
import java.util.concurrent.atomic.AtomicLong

import com.lakala.finance.stream.screen.common.DateTimeUtils
import org.apache.spark.Logging
import redis.clients.jedis.JedisCluster

/**
  * Created by longxiaolei on 2017/3/30.
  */
object BizCleaner extends Logging{
  /*清理次数标记*/
  var cleanTime: AtomicLong = new AtomicLong(0)

  /**
    * 清理信贷和基金的所有的水位指针，以及其对应的锁
    *
    * @param end_suffix
    * @return
    */
  def cleanPointerAndLock(jedisCluster: JedisCluster, end_suffix: String) = {
    val water_point: String = "dataPlatform.water_pointer" + end_suffix
    val lock: String = "dataPlatform.clean_lock" + end_suffix
//    jedisCluster.del(water_point, lock)
    cleanKeys(jedisCluster,water_point,lock)
    logWarning("成功清理了水位和锁")
  }

  def cleanSubOldData(jedisCluster: JedisCluster, timeStamp: Long, dayStr: String, end_suffix: String) = {
    //清理历史结果
    val sub_apply_count: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + dayStr + "." + Constant.Redis.SUBSECTION_COUNT_SUFFIX + end_suffix
    val sub_loan_count: String = Constant.Redis.PREFIX + Constant.LogNo.LOAN_FLAG + "." + dayStr + "." + Constant.Redis.SUBSECTION_COUNT_SUFFIX + end_suffix
    val sub_apply_amount: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + dayStr + "." + Constant.Redis.SUBSECTION_AMOUNT_SUFFIX + end_suffix
    val sub_loan_amount: String = Constant.Redis.PREFIX + Constant.LogNo.LOAN_FLAG + "." + dayStr + "." + Constant.Redis.SUBSECTION_AMOUNT_SUFFIX + end_suffix

    //清理昨天的计算结果
    val yesterdayStr: String = DateTimeUtils.getYesterdayStr(dayStr)
    val sub_apply_count_yesterday: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + yesterdayStr + "." + Constant.Redis.SUBSECTION_COUNT_SUFFIX + end_suffix
    val sub_loan_count_yesterday: String = Constant.Redis.PREFIX + Constant.LogNo.LOAN_FLAG + "." + yesterdayStr + "." + Constant.Redis.SUBSECTION_COUNT_SUFFIX + end_suffix
    val sub_apply_amount_yesterday: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + yesterdayStr + "." + Constant.Redis.SUBSECTION_AMOUNT_SUFFIX + end_suffix
    val sub_loan_amount_yesterday: String = Constant.Redis.PREFIX + Constant.LogNo.LOAN_FLAG + "." + yesterdayStr + "." + Constant.Redis.SUBSECTION_AMOUNT_SUFFIX + end_suffix

    cleanKeys(jedisCluster,sub_apply_count, sub_loan_count, sub_apply_amount, sub_loan_amount,
      sub_apply_count_yesterday, sub_loan_count_yesterday, sub_apply_amount_yesterday, sub_loan_amount_yesterday)
    //将当前数据的水位记录到redis中，以便离线任务正确的跑数
  }

  def cleanLocationOldData(jedisCluster: JedisCluster, timeStamp: Long, dayStr: String, end_suffix: String) = {
    //清除需要历史的结果
    val city_key: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + dayStr + "." + Constant.Redis.CITY_DAILY_COUNT + end_suffix
    val province_key: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + dayStr + "." + Constant.Redis.PROVINCE_DAILY_COUNT + end_suffix

    //清理昨天的计算结果
    val yesterdayStr: String = DateTimeUtils.getYesterdayStr(dayStr)
    val city_key_yesterday: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + yesterdayStr + "." + Constant.Redis.CITY_DAILY_COUNT + end_suffix
    val province_key_yesterday: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + yesterdayStr + "." + Constant.Redis.PROVINCE_DAILY_COUNT + end_suffix

    cleanKeys(jedisCluster,city_key, province_key, city_key_yesterday, province_key_yesterday)
    //            RedisUtils.cleanDailyResult(dayStr)
    //将当前数据的水位记录到redis中，以便离线任务正确的跑数
  }

  def cleanDailyOldData(jedisCluster: JedisCluster, timeStamp: Long, dayStr: String, end_suffix: String) = {
    //清除第一条数据对应的当天的结果
    val daily_apply_count: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + dayStr + "." + Constant.Redis.DAILY_COUNT_SUFFIX + end_suffix
    val daily_loan_count: String = Constant.Redis.PREFIX + Constant.LogNo.LOAN_FLAG + "." + dayStr + "." + Constant.Redis.DAILY_COUNT_SUFFIX + end_suffix
    val daily_apply_amount: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + dayStr + "." + Constant.Redis.DAILY_AMOUNT_SUFFIX + end_suffix
    val daily_loan_amount: String = Constant.Redis.PREFIX + Constant.LogNo.LOAN_FLAG + "." + dayStr + "." + Constant.Redis.DAILY_AMOUNT_SUFFIX + end_suffix

    //清理昨天的计算结果
    val yesterdayStr: String = DateTimeUtils.getYesterdayStr(dayStr)
    val daily_apply_count_yesterday: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + yesterdayStr + "." + Constant.Redis.DAILY_COUNT_SUFFIX + end_suffix
    val daily_loan_count_yesterday: String = Constant.Redis.PREFIX + Constant.LogNo.LOAN_FLAG + "." + yesterdayStr + "." + Constant.Redis.DAILY_COUNT_SUFFIX + end_suffix
    val daily_apply_amount_yesterday: String = Constant.Redis.PREFIX + Constant.LogNo.APPLY_FLAG + "." + yesterdayStr + "." + Constant.Redis.DAILY_AMOUNT_SUFFIX + end_suffix
    val daily_loan_amount_yesterday: String = Constant.Redis.PREFIX + Constant.LogNo.LOAN_FLAG + "." + yesterdayStr + "." + Constant.Redis.DAILY_AMOUNT_SUFFIX + end_suffix

    cleanKeys(jedisCluster,daily_apply_count, daily_loan_count, daily_apply_amount, daily_loan_amount,
      daily_apply_count_yesterday, daily_loan_count_yesterday, daily_apply_amount_yesterday, daily_loan_amount_yesterday)
    //将当前数据的水位记录到redis中，以便离线任务正确的跑数
  }

  def cleanFundData(jedisCluster: JedisCluster, timeStamp: Long, dayStr: String, end_suffix: String) = {
    val yesterdayStr: String = DateTimeUtils.getYesterdayStr(dayStr)
    val fund_count_key = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FUND_COUNT + end_suffix
    val fund_amount_key = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FUND_AMOUNT + end_suffix
    val financing_count_key = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FINANCING_COUNT + end_suffix
    val financing_amount_key = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FINANCING_AMOUNT + end_suffix
    val fund_financing_total_count_key = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FUND_FINANCING_TOTAL_COUNT + end_suffix
    val fund_financing_total_amount_key = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + dayStr + "." + Constant.Redis.FUND_FINANCING_TOTAL_AMOUNT + end_suffix

    val fund_count_key_yesterday = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + yesterdayStr + "." + Constant.Redis.FUND_COUNT + end_suffix
    val fund_amount_key_yesterday = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + yesterdayStr + "." + Constant.Redis.FUND_AMOUNT + end_suffix
    val financing_count_key_yesterday = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + yesterdayStr + "." + Constant.Redis.FINANCING_COUNT + end_suffix
    val financing_amount_key_yesterday = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + yesterdayStr + "." + Constant.Redis.FINANCING_AMOUNT + end_suffix
    val fund_financing_total_count_key_yesterday = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + yesterdayStr + "." + Constant.Redis.FUND_FINANCING_TOTAL_COUNT + end_suffix
    val fund_financing_total_amount_key_yesterday = Constant.Redis.PREFIX + Constant.Redis.FINANCING_INFIX + "." + yesterdayStr + "." + Constant.Redis.FUND_FINANCING_TOTAL_AMOUNT + end_suffix

    cleanKeys(jedisCluster,fund_count_key, fund_amount_key, financing_count_key, financing_amount_key, fund_financing_total_count_key, fund_financing_total_amount_key,
      fund_count_key_yesterday, fund_amount_key_yesterday, financing_count_key_yesterday, financing_amount_key_yesterday, fund_financing_total_count_key_yesterday, fund_financing_total_amount_key_yesterday)
  }

  def cleanKeys(jedisCluster: JedisCluster, keys: String*) = {
    keys.foreach(key=>{
      jedisCluster.del(key)
    })
  }
}
