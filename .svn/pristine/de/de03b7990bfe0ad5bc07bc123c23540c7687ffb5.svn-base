package com.lakala.finance.stream.screen.biz

import java.util

import com.lakala.finance.common.EnvUtil
import org.apache.spark.Logging
import redis.clients.jedis.JedisCluster

/**
  * Created by longxiaolei on 2017/3/30.
  */
object BizWriter extends Logging {

  def writeValue2Reids(jedisCluster: JedisCluster, flag: String, flag_value: String, dayStr: String, value: Long, end_suffix: String) = {
    //每天总数统计
    if (Constant.Flag.DAILY_APPLY_COUNT.equals(flag)) {
      writeKey2Redis(jedisCluster,s"dataPlatform.generalApply_CreditLoan.${dayStr}.totalOneDay${end_suffix}",value)
    } else if (Constant.Flag.DAILY_APPLY_AMOUNT.equals(flag)) {
      writeKey2Redis(jedisCluster,s"dataPlatform.generalApply_CreditLoan.${dayStr}.amountOneDay${end_suffix}",value)
    } else if (Constant.Flag.DAILY_LOAN_COUNT.equals(flag)) {
      writeKey2Redis(jedisCluster,s"dataPlatform.CreditLoan_WorkFlow_LoanAmount.${dayStr}.totalOneDay${end_suffix}",value)
    } else if (Constant.Flag.DAILY_LOAN_AMOUNT.equals(flag)) {
      writeKey2Redis(jedisCluster,s"dataPlatform.CreditLoan_WorkFlow_LoanAmount.${dayStr}.amountOneDay${end_suffix}",value)

      //5分钟地图
    } else if (Constant.Flag.SUB_APPLY_COUNT.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.generalApply_CreditLoan.${dayStr}.totalTimeSubsection${end_suffix}", flag_value, value)
    } else if (Constant.Flag.SUB_APPLY_AMOUNT.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.generalApply_CreditLoan.${dayStr}.amountTimeSubsection${end_suffix}", flag_value, value)
    } else if (Constant.Flag.SUB_LOAN_COUNT.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.CreditLoan_WorkFlow_LoanAmount.${dayStr}.totalTimeSubsection${end_suffix}", flag_value, value)
    } else if (Constant.Flag.SUB_LOAN_AMOUNT.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.CreditLoan_WorkFlow_LoanAmount.${dayStr}.amountTimeSubsection${end_suffix}", flag_value, value)

      //城市和省份
    } else if (Constant.Flag.PROVINCE_APPLY_COUNT.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.generalApply_CreditLoan.${dayStr}.totalProvinceOneDay${end_suffix}", flag_value, value)
    } else if (Constant.Flag.CITY_APPLY_COUNT.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.generalApply_CreditLoan.${dayStr}.totalCityOneDay${end_suffix}", flag_value, value)
    }

    //基金和理财统计
    else if (Constant.Flag.FUND_COUNT.equals(flag)) {
      writeKey2Redis(jedisCluster,s"dataPlatform.bamdata.${dayStr}.financingCurrentCount${end_suffix}",value)
    } else if (Constant.Flag.FUND_AMOUNT.equals(flag)) {
      writeKey2Redis(jedisCluster,s"dataPlatform.bamdata.${dayStr}.financingCurrentAmount${end_suffix}",value)
    } else if (Constant.Flag.FINACING_COUNT.equals(flag)) {
      writeKey2Redis(jedisCluster,s"dataPlatform.bamdata.${dayStr}.financingRegularCount${end_suffix}",value)
    } else if (Constant.Flag.FINACING_AMOUNT.equals(flag)) {
      writeKey2Redis(jedisCluster,s"dataPlatform.bamdata.${dayStr}.financingRegularAmount${end_suffix}",value)
    } else if (Constant.Flag.FUND_FINACING_TOTAL_COUNT.equals(flag)) {
      writeKey2Redis(jedisCluster,s"dataPlatform.bamdata.${dayStr}.financingTotalCount${end_suffix}",value)
    } else if (Constant.Flag.FUND_FINACING_TOTAL_AMOUNT.equals(flag)) {
      writeKey2Redis(jedisCluster,s"dataPlatform.bamdata.${dayStr}.financingTotalAmount${end_suffix}",value)
    }

    // 添加风控相关业务代码
    else if (Constant.Flag.RISK_REJECT_REASON.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.RejectReason${end_suffix}", flag_value, value)
    } else if (Constant.Flag.RISK_CUSTOMER_TYPE_COUNT.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.CustomerType${end_suffix}", flag_value, value)
    } else if (Constant.Flag.RISK_CITY_COUNT.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.HighRiskArea.city${end_suffix}", flag_value, value)
    } else if (Constant.Flag.RISK_PROVINCE_COUNT.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.HighRiskArea.province${end_suffix}", flag_value, value)
    } else if (Constant.Flag.RISK_LEVEL.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.RiskLevel${end_suffix}", flag_value, value)
    } else if (Constant.Flag.RISK_CREDIT_SCORE.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.CreditScore${end_suffix}", flag_value, value)
    } else if (Constant.Flag.RISK_DECISION_RESULT.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.DecisionResults${end_suffix}", flag_value, value)
    } else if (Constant.Flag.RISK_K_SCORE.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.LongBorrowing${end_suffix}", flag_value, value)
    } else if (Constant.Flag.RISK_CHANNEL.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.ChannelApplication${end_suffix}", flag_value, value)
    } else if (Constant.Flag.RISK_CUSTOMER_DISTRIBUTION.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.customer.distribution${end_suffix}", flag_value, value)
    } else if (Constant.Flag.RISK_SYSTEM_APPROVE.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.system${end_suffix}", flag_value, value)
    } else if (Constant.Flag.RISK_MANUAL_APPROVE.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.manual${end_suffix}", flag_value, value)
    } else if (Constant.Flag.RISK_REJECT_CODE.equals(flag)) {
      writeHset2Redis(jedisCluster, s"dataPlatform.bamdata.${dayStr}.manualRejectReason${end_suffix}", flag_value, value)
    }
  }

  private def writeHset2Redis(jedisCluster: JedisCluster, hsetKey: String, key: String, value: Long) = {
    jedisCluster.hincrBy(hsetKey, key, value)
    jedisCluster.expire(hsetKey, Constant.Redis.EXPIRE)
  }

  private def writeKey2Redis(jedisCluster: JedisCluster,key:String,value: Long)={
    jedisCluster.incrBy(key,value)
    jedisCluster.expire(key, Constant.Redis.EXPIRE)
  }

}
