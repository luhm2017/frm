package com.lakala.finance.stream.screen.biz

import java.sql.{PreparedStatement, ResultSet}

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.lakala.finance.stream.screen.common.CommonUtils
import org.apache.spark.Logging

import scala.collection.mutable.ArrayBuffer

/**
  * Created by longxiaolei on 2017/2/23.
  */
object BizSql extends Logging {
  def query_fund_info(mysqlDS:DruidDataSource,query_flag: String, startTime: String, endTime: String): ArrayBuffer[(Long, Long, String)] = {
    val con: DruidPooledConnection = mysqlDS.getConnection
    var ps: PreparedStatement = null
    if ("fund".equals(query_flag)) {
      ps = con.prepareStatement(Constant.SQL.QUERY_FUND)
    } else if ("finacing".equals(query_flag)) {
      ps = con.prepareStatement(Constant.SQL.QUERY_FINACING)
    } else {
      println("请传入正确的查询标记方可查询")
      return null
    }
    ps.setString(1, startTime)
    ps.setString(2, endTime)
    val query: ResultSet = ps.executeQuery()
    val result: ArrayBuffer[(Long, Long, String)] = new ArrayBuffer[(Long, Long, String)]()
    while (query.next()) {
      val count: Long = query.getLong(1)
      val amount: Long = query.getLong(2)
      val dayStr: String = query.getString(3)
      result.+=((count, amount, dayStr))
    }
    result
  }

  def query_applyOrloan_info(mysqlDS:DruidDataSource,query_flag: String, startTime: String, endTime: String): ArrayBuffer[(Long, Long, String)] = {
    val con: DruidPooledConnection = mysqlDS.getConnection
    var ps: PreparedStatement = null
    try{
      if ("apply".equals(query_flag)) {
        ps = con.prepareStatement(Constant.SQL.QUERY_APPLY_DAILY)
      } else if ("loan".equals(query_flag)) {
        ps = con.prepareStatement(Constant.SQL.QUERY_LOAN_DAILY)
      } else {
        println("请传入正确的查询标记方可查询")
        return null
      }
      ps.setString(1, startTime)
      ps.setString(2, endTime)
      val query: ResultSet = ps.executeQuery()
      val result = new ArrayBuffer[(Long, Long, String)]()
      while (query.next()) {
        val count: Long = query.getLong(1)
        val amount: Long = query.getLong(2)
        val dayStr: String = query.getString(3)
        result.+=((count, amount, dayStr))
      }
      result
    } finally {
      closeConAndPs(con, ps)
    }
  }

  /**
    * @param query_flag 类型标记： apply表示查询进件信息，loan表示查询审批信息
    * @param startTime
    * @param endTime
    * @return ArrayBuffer[ (amount,collection_time)] collect_time 对应的格式为 yyyy-MM-dd HH:mm:ss
    */
  def query_apply_or_loan_info(mysqlDS:DruidDataSource,query_flag: String, startTime: String, endTime: String): ArrayBuffer[(Long, String, String, String)] = {
    val con: DruidPooledConnection = mysqlDS.getConnection
    var ps: PreparedStatement = null
    try {
      if ("apply".equals(query_flag)) {
        ps = con.prepareStatement(Constant.SQL.QUERY_APPLY)
      } else if ("loan".equals(query_flag)) {
        ps = con.prepareStatement(Constant.SQL.QUERY_LOAN)
      } else {
        println("请传入正确的查询标记方可查询")
        return null
      }
      ps.setString(1, startTime)
      ps.setString(2, endTime)
      val query: ResultSet = ps.executeQuery()
      val result = new ArrayBuffer[(Long, String, String, String)]()
      var amount_Str: String = null
      var amount: Long = 0l
      var collectTime: String = null
      var province: String = "default"
      var city: String = "default"
      var provinceStr: String = null
      var cityStr: String = null
      while (query.next()) {
        amount_Str = query.getString(1)
        if (!CommonUtils.isNullOrEmpty(amount_Str)) {
          amount = amount_Str.toLong
        }
        collectTime = query.getString(2)
        provinceStr = query.getString(3)
        if (!CommonUtils.isNullOrEmpty(provinceStr)) {
          province = provinceStr
        }
        cityStr = query.getString(4)
        if (!CommonUtils.isNullOrEmpty(cityStr)) {
          city = cityStr.replace("市", "")
        }
        result += ((amount, collectTime.substring(0, collectTime.length - 2), province, city))
      }
      result
    } finally {
      closeConAndPs(con, ps)
    }
  }

  def writeErrorJson2DB(ps: PreparedStatement, time: String, taskName: String, exception_info: String, json: String) = {
    try {
      ps.setString(1, time)
      ps.setString(2, taskName)
      ps.setString(3, exception_info)
      ps.setString(4, json)
      ps.addBatch()
      //      ps.execute()
    } catch {
      case ex: Exception =>
        val erroInfo: String = CommonUtils.getExceptionInfo(ex)
        logError(erroInfo)
    }
  }

  def closeConAndPs(con: DruidPooledConnection, ps: PreparedStatement) = {
    if (con != null) {
      con.close()
    }
    if (ps != null) {
      ps.close()
    }
  }

}
