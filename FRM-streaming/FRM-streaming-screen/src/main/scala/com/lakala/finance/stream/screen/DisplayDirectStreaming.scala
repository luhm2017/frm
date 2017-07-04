package com.lakala.finance.stream.screen

import java.sql.PreparedStatement
import java.util.Date

import com.alibaba.druid.pool.{DruidDataSource, DruidPooledConnection}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.lakala.finance.common.EnvUtil
import com.lakala.finance.stream.screen.biz.{BizCleaner, BizSql, BizWriter, Constant}
import com.lakala.finance.stream.screen.common.{CommonUtils, DateTimeUtils}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManger, OffsetRange}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

object DisplayDirectStreaming extends Logging {


  def main(args: Array[String]) {
    //入参校验
    assertArgs(args)
    val env: String = args(0)
    val topics: String = args(1)
    val group: String = args(2)
    val seconds: String = args(3)
    val ckdir: String = args(4)
    var isClean: Boolean = false
    if (args.length == 6) {
      isClean = args(5).toBoolean
    }

    //判断是否是生产的测试环境，因为生产的测试环境和真实的生产环境对应的环境信息相同，而对应的rediskey值不一样
    val env_suffix: String = if (env.endsWith("_test")) "_test" else ""

    val batchTime: Long = seconds.toLong
    val topicSet: Set[String] = topics.split(",").toSet

    val conf: SparkConf = new SparkConf().setAppName("CreditLoanDirectStream" + env_suffix)
    conf.set("spark.streaming.concurrentJobs", "2") //设置streaming并行job的参数
    conf.set("spark.streaming.backpressure.enabled", "true") //设置streaming开启背压机制
    conf.set("spark.ui.showConsoleProgress", "false") //在控制台不打印每个任务的箭头完成度信息
    conf.set("spark.locality.wait", "1") //设置任务调度等待间隔为0
    conf.set("spark.streaming.kafka.maxRetries", "100") //设置kafka异常的时候重连kafka信息的次数 每次重连等待的时间通过"refresh.leader.backoff.ms"设置，默认是200ms
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //设置序列化为kryo
    conf.set("refresh.leader.backoff.ms", "1000")
    //    conf.set("spark.speculation", "true") //开启推导执行
    //    conf.set("spark.speculation.multiplier", "10") //推导执行的启动时间
    //    conf.setMaster("local[2]")

    val sc: SparkContext = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Milliseconds(batchTime))
    sc.setLogLevel("WARN")
    ssc.checkpoint(ckdir)

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> EnvUtil.kafkaBrokers(env),
      "group.id" -> group
    )

    val km: KafkaManger = new KafkaManger(kafkaParams, env)

    if (isClean) {
      //清理所有的topic在redis中记录的偏移量
      km.delOffsetInRedis(topicSet, group)

      //清理业务对应的水位指针以及删除锁
      val jedisCluster: JedisCluster = EnvUtil.jedisCluster(env)
      BizCleaner.cleanPointerAndLock(jedisCluster, env_suffix)
    }

    var offsetRanges = Array[OffsetRange]()
    //设置streaming的监听器，用来跟新偏移量
    //    ssc.addStreamingListener(new OffsetStreamingListener(envFlag, kafkaParams, offsetRanges))

    val directKafkaStream: InputDStream[(String, String)] = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, topicSet)

    //获取偏移量信息
    val transform: DStream[(String, String)] = directKafkaStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    val data: DStream[((String, String, String), Long)] = transform.flatMap { case (key, value) =>
      try {
        val jsonObject: JSONObject = JSON.parseObject(value)
        val logNo: String = jsonObject.getString("logNo")

        val timeStamp: Long = jsonObject.getLong("collectTime")

        //解析时间相关信息
        val dayStr: String = DateTimeUtils.getDayStr(timeStamp)
        val intervalTimeStamp: Long = DateTimeUtils.getIntervalTimeStr(timeStamp)

        if (isClean) {
          cleanHistoryData(env, env_suffix, timeStamp, dayStr)
        }

        val result: ArrayBuffer[((String, String, String), Long)] = new ArrayBuffer[((String, String, String), Long)]()
        if (Constant.LogNo.APPLY_FLAG.equals(logNo)) {
          //进件
          doApplyLogic(jsonObject, result, intervalTimeStamp, dayStr)
        } else if (Constant.LogNo.LOAN_FLAG.equals(logNo)) {
          //放款
          doLoanLogic(jsonObject, dayStr, intervalTimeStamp, result, env)
        } else if (Constant.LogNo.FUND_FLAG.equals(logNo)) {
          //只计算申购的数量
          doFundLogic(jsonObject, dayStr, result)
        } else if (Constant.LogNo.FINACING_FLAG.equals(logNo)) {
          //只计算申购的数量
          doFinacingLogic(jsonObject, result, dayStr)
        } else if (Constant.LogNo.RISK_CONTROL.equals(logNo)) {
          //风控业务
          doRiskLogic(jsonObject, result, dayStr, env)
        } else if (Constant.LogNo.AUDIT_RESULT.equals(logNo)) {
          //统计风控拒绝原因
          doAuditLogic(jsonObject, result, dayStr)
        }
        else {
          result.+=(((logNo, "", dayStr), 0l))
        }
        result
      } catch {
        case ex: Exception =>
          val errorInfo: String = CommonUtils.getExceptionInfo(ex)
          // 因为在各个excutor上没有执行过加载逻辑，所以要先执行加载逻辑
          logError(errorInfo)
          ArrayBuffer(((Constant.Flag.EX, errorInfo, value), 0l))
      }
    }.cache()

    //将统计结果入库
    data.filter(x => null != x && !Constant.Flag.EX.equals(x._1._1))
      .reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          //因为数据量不大，做一个针对性的优化，避免每次都要写redis的情况
          try {
            val jedisCluster: JedisCluster = EnvUtil.jedisCluster(env)
            it.foreach(x => {
              val flag: String = x._1._1
              val flag_value: String = x._1._2
              val dayStr: String = x._1._3
              val value: Long = x._2
              //根据flay中的类型讲值写入redis中
              BizWriter.writeValue2Reids(jedisCluster, flag, flag_value, dayStr, value, env_suffix)
            })
            CommonUtils.writeOffsets2Redis(env, group, offsetRanges)
          } catch {
            case ex: Exception =>
              CommonUtils.logException(ex)
          }
        })
      })

    //将错误信息写入mysql
    data.filter(x => null != x && Constant.Flag.EX.equals(x._1._1))
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          val dataResource: DruidDataSource = EnvUtil.mysqlDS(env)
          var conn: DruidPooledConnection = null
          var ps: PreparedStatement = null
          try {
            conn = dataResource.getConnection
            ps = conn.prepareStatement(Constant.SQL.ERRORINFO_2_DB_SQL)
            val time: String = DateTimeUtils.getCurrentTime()
            it.foreach(x => {
              BizSql.writeErrorJson2DB(ps, time, "CreditFundDirectStream" + env_suffix, x._1._2, x._1._3)
            })
            ps.executeBatch()
          } catch {
            case ex: Exception =>
              CommonUtils.logException(ex)
          } finally {
            BizSql.closeConAndPs(conn, ps)
          }
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 清理历史数据
    *
    * @param envFlag
    * @param end_suffix
    * @param timeStamp
    * @param dayStr
    * @return
    */
  def cleanHistoryData(envFlag: String, end_suffix: String, timeStamp: Long, dayStr: String): AnyVal = {
    //根据广播变量判断是否需要清理计算结果，并设置计算水位
    val cleanTime = BizCleaner.cleanTime.addAndGet(1)
    //只有第一次才需要清理，避免重复清理的逻辑
    if (1 == cleanTime) {
      //设置水位，并清理历史数据
      val jedisCluster: JedisCluster = EnvUtil.jedisCluster(envFlag)

      val waterpointer: String = "dataPlatform.water_pointer" + end_suffix

      logWarning("设置水位时间为：" + new Date(timeStamp))

      jedisCluster.sadd(waterpointer, timeStamp.toString)

      val lock: String = "dataPlatform.clean_lock" + end_suffix
      val nx: Long = jedisCluster.setnx(lock, timeStamp.toString)
      jedisCluster.expire("dataPlatform.clean_lock", 30)

      if (nx == 1) {
        logWarning("开始清理每天的进件统计。。。")
        BizCleaner.cleanDailyOldData(jedisCluster, timeStamp, dayStr, end_suffix)
        logWarning("开始清理每天的进件的地区统计。。。")
        BizCleaner.cleanLocationOldData(jedisCluster, timeStamp, dayStr, end_suffix)
        logWarning("开始清理每五分钟的地区统计。。。")
        BizCleaner.cleanSubOldData(jedisCluster, timeStamp, dayStr, end_suffix)
        logWarning("开始清理基金和理财的统计。。。")
        BizCleaner.cleanFundData(jedisCluster, timeStamp, dayStr, end_suffix)
        //todo 清理风控生产历史数据

        jedisCluster.del(lock)
        logWarning("历史数据清理完毕")
      }
    }
  }

  private def doApplyLogic(jsonObject: JSONObject, result: ArrayBuffer[((String, String, String), Long)], intervalTimeStamp: Long, dayStr: String) = {
    // 获取省市信息
    val flatMap: JSONObject = jsonObject.getJSONObject("flatMap")
    val city: String = getCityInfo(flatMap, flatMap.getString("lbsCity"))
    val province: String = getProvince(flatMap, flatMap.getString("lbsProvince"))

    //获取金额的信息
    val applyAmt = flatMap.getString("applyamt").trim
    val amount: Long = if (CommonUtils.isNullOrEmpty(applyAmt)) 0l else applyAmt.toLong
    //统计总数
    result.+=(((Constant.Flag.DAILY_APPLY_COUNT, "", dayStr), 1l))
    result.+=(((Constant.Flag.DAILY_APPLY_AMOUNT, "", dayStr), amount))
    //统计每5分钟的指标
    result.+=(((Constant.Flag.SUB_APPLY_COUNT, intervalTimeStamp.toString, dayStr), 1l))
    result.+=(((Constant.Flag.SUB_APPLY_AMOUNT, intervalTimeStamp.toString, dayStr), amount))
    //统计地图信息
    result.+=(((Constant.Flag.PROVINCE_APPLY_COUNT, province, dayStr), 1l))
    result.+=(((Constant.Flag.CITY_APPLY_COUNT, city, dayStr), 1l))

    var channelCode: String = flatMap.getString("channelcode")
    if (!CommonUtils.isNullOrEmpty(channelCode)) {
      channelCode = channelCode.trim
      result.+=(((Constant.Flag.RISK_CHANNEL, channelCode, dayStr), 1l))
    } else {
      val source: String = flatMap.getString("channelsource")
      if ("A".equals(source)) {
        result.+=(((Constant.Flag.RISK_CHANNEL, "lakalaApp", dayStr), 1l))
      }
    }
  }

  private def doLoanLogic(jsonObject: JSONObject, dayStr: String, intervalTimeStamp: Long, result: ArrayBuffer[((String, String, String), Long)], env: String) = {
    //获取放款信息
    val flatMap: JSONObject = jsonObject.getJSONObject("flatMap")
    val capitalAmount = flatMap.getString("capitalAmount").trim
    val amount: Long = if (CommonUtils.isNullOrEmpty(capitalAmount)) 0l else capitalAmount.toLong
    //统计总数
    result.+=(((Constant.Flag.DAILY_LOAN_COUNT, "", dayStr), 1l))
    result.+=(((Constant.Flag.DAILY_LOAN_AMOUNT, "", dayStr), amount))
    //统计每5分钟的指标
    result.+=(((Constant.Flag.SUB_LOAN_COUNT, intervalTimeStamp.toString, dayStr), 1l))
    result.+=(((Constant.Flag.SUB_LOAN_AMOUNT, intervalTimeStamp.toString, dayStr), amount))

    val orderId: String = flatMap.getString("orderId")
    if (!CommonUtils.isNullOrEmpty(orderId)) {
      val jedisCluster: JedisCluster = EnvUtil.jedisCluster(env)
      val customType: String = jedisCluster.get(s"dataPlatform.RiskOrderType.${orderId}")
      if (!CommonUtils.isNullOrEmpty(customType)) {
        result.+=(((Constant.Flag.RISK_CUSTOMER_DISTRIBUTION, s"${customType}.amount", dayStr), amount))
      }
    }

  }

  private def doFundLogic(jsonObject: JSONObject, dayStr: String, result: ArrayBuffer[((String, String, String), Long)]) = {
    val flatMap: JSONObject = jsonObject.getJSONObject("flatMap")
    val json_value: Long = flatMap.getLong("amount")
    val amount: Long = if (null != json_value) json_value else 0l
    //基金
    if ("1G2".equals(flatMap.getString("busid"))) {
      result.+=(((Constant.Flag.FUND_COUNT, "", dayStr), 1l))
      result.+=(((Constant.Flag.FUND_AMOUNT, "", dayStr), amount))

      //记为总数
      result.+=(((Constant.Flag.FUND_FINACING_TOTAL_COUNT, "", dayStr), 1l))
      result.+=(((Constant.Flag.FUND_FINACING_TOTAL_AMOUNT, "", dayStr), amount))
    } else {
      result.+=(((Constant.Flag.FUND_FINACING_TOTAL_COUNT, "", dayStr), 0l))
    }
  }

  private def doFinacingLogic(jsonObject: JSONObject, result: ArrayBuffer[((String, String, String), Long)], dayStr: String) = {
    val flatMap: JSONObject = jsonObject.getJSONObject("flatMap")
    val json_value: Long = flatMap.getLong("amount")
    val amount: Long = if (null != json_value) json_value else 0l
    //理财
    result.+=(((Constant.Flag.FINACING_COUNT, "", dayStr), 1l))
    result.+=(((Constant.Flag.FINACING_AMOUNT, "", dayStr), amount))
    //记为总数
    result.+=(((Constant.Flag.FUND_FINACING_TOTAL_COUNT, "", dayStr), 1l))
    result.+=(((Constant.Flag.FUND_FINACING_TOTAL_AMOUNT, "", dayStr), amount))
  }

  def doAuditLogic(jsonObject: JSONObject, result: ArrayBuffer[((String, String, String), Long)], dayStr: String) = {
    val flatMap: JSONObject = jsonObject.getJSONObject("flatMap")

    //(人工和系统)审批拒绝量和通过量 埋点：	CreditLoan_WorkFlow_AuditResult  字段： APPROVE_TYPE（SA-系统审批；MA-人工审批）
    val approveType: String = flatMap.getString("APPROVE_TYPE")
    val approveResult: String = flatMap.getString("A_RESULT")
    if (!CommonUtils.isNullOrEmpty(approveType)) {
      val flag =
        if ("SA".equals(approveType)) Constant.Flag.RISK_SYSTEM_APPROVE //系统审批
        else if ("MA".equals(approveType)) Constant.Flag.RISK_MANUAL_APPROVE //人工审批
        else null

      val flag_value =
        if ("AD".equals(approveResult)) "refuse.count"
        else if ("AP".equals(approveResult)) "pass.count"
        else null

      if (flag != null && flag_value != null) {
        result.+=(((flag, flag_value, dayStr), 1l))
      }
    }

    //人工审批拒绝原因申请量
    val rejectCode: String = flatMap.getString("A_REJECT_CODE")
    if (!CommonUtils.isNullOrEmpty(rejectCode)) {
      val rejectResult: String = rejectCode.split(",")(0)
      result.+=(((Constant.Flag.RISK_REJECT_CODE, rejectResult, dayStr), 1l))
    }
  }

  private def doRiskLogic(jsonObject: JSONObject, result: ArrayBuffer[((String, String, String), Long)], dayStr: String, env: String) = {
    val flatMap: JSONObject = jsonObject.getJSONObject("flatMap")
    var amount = flatMap.getLong("Z_PERMIT_LMT")
    amount = if (null == amount) 0l else amount

    //命中规则 "Z_REJECT_CODE": "Y66|X69|"  命中规则 取| 分割的第一个
    val reject_code_str: String = flatMap.getString("Z_REJECT_CODE")
    if (!CommonUtils.isNullOrEmpty(reject_code_str)) {
      //设置命中规则的业务
      val codeArray: Array[String] = reject_code_str.split("\\|")
      if (codeArray.length > 0) {
        val rejectCode = codeArray(0)
        result.+=(((Constant.Flag.RISK_REJECT_REASON, rejectCode, dayStr), 1l))
      }
    }

    //风险等级 Z_RESULT_VAR104    1-极高风险 ， 2-高风险，3-中风险，4-低风险，5-极地风险
    val risk_level = flatMap.getFloat("Z_RESULT_VAR104")
    if (null != risk_level) {
      if (risk_level == 1.0) {
        result.+=(((Constant.Flag.RISK_LEVEL, "max.count", dayStr), 1l))
        result.+=(((Constant.Flag.RISK_LEVEL, "max.amount", dayStr), amount))
        writeRiskLocation(result, dayStr, flatMap) //需要写入高风险地区
      } else if (risk_level == 2.0) {
        result.+=(((Constant.Flag.RISK_LEVEL, "high.count", dayStr), 1l))
        result.+=(((Constant.Flag.RISK_LEVEL, "high.amount", dayStr), amount))
        writeRiskLocation(result, dayStr, flatMap) //需要写入高风险地区
      } else if (risk_level == 3.0) {
        result.+=(((Constant.Flag.RISK_LEVEL, "mid.count", dayStr), 1l))
        result.+=(((Constant.Flag.RISK_LEVEL, "mid.amount", dayStr), amount))
      } else if (risk_level == 4.0) {
        result.+=(((Constant.Flag.RISK_LEVEL, "low.count", dayStr), 1l))
        result.+=(((Constant.Flag.RISK_LEVEL, "low.amount", dayStr), amount))
      } else if (risk_level == 5.0) {
        result.+=(((Constant.Flag.RISK_LEVEL, "min.count", dayStr), 1l))
        result.+=(((Constant.Flag.RISK_LEVEL, "min.amount", dayStr), amount))
      }
    }

    //风控结果 "Z_RESULT_VAR014": "AD"  结果  AD:拒绝 RR:人工，AP：通过
    val risk_result_str: String = flatMap.getString("Z_RESULT_VAR014")
    if (!CommonUtils.isNullOrEmpty(risk_result_str)) {
      if ("AD".equals(risk_result_str.trim)) {
        result.+=(((Constant.Flag.RISK_DECISION_RESULT, "refuse.count", dayStr), 1l))
        result.+=(((Constant.Flag.RISK_DECISION_RESULT, "refuse.amount", dayStr), amount))
      } else if ("RR".equals(risk_result_str.trim)) {
        result.+=(((Constant.Flag.RISK_DECISION_RESULT, "manual.count", dayStr), 1l))
        result.+=(((Constant.Flag.RISK_DECISION_RESULT, "manual.amount", dayStr), amount))
      } else if ("AP".equals(risk_result_str.trim)) {
        result.+=(((Constant.Flag.RISK_DECISION_RESULT, "pass.count", dayStr), 1l))
        result.+=(((Constant.Flag.RISK_DECISION_RESULT, "pass.amount", dayStr), amount))
      }
    }

    // Z_RESULT_VAR030 评分
    val score = flatMap.getDouble("Z_RESULT_VAR030")
    if (score != null) {
      if (score >= 0 && score < 300) {
        result.+=(((Constant.Flag.RISK_CREDIT_SCORE, "0to300.count", dayStr), 1l))
        result.+=(((Constant.Flag.RISK_CREDIT_SCORE, "0to300.amount", dayStr), amount))
      } else if (score >= 300 && score < 500) {
        result.+=(((Constant.Flag.RISK_CREDIT_SCORE, "300to500.count", dayStr), 1l))
        result.+=(((Constant.Flag.RISK_CREDIT_SCORE, "300to500.amount", dayStr), amount))
      } else if (score >= 500 && score < 600) {
        result.+=(((Constant.Flag.RISK_CREDIT_SCORE, "500to600.count", dayStr), 1l))
        result.+=(((Constant.Flag.RISK_CREDIT_SCORE, "500to600.amount", dayStr), amount))
      } else if (score >= 600 && score <= 750) {
        result.+=(((Constant.Flag.RISK_CREDIT_SCORE, "600to750.count", dayStr), 1l))
        result.+=(((Constant.Flag.RISK_CREDIT_SCORE, "600to750.amount", dayStr), amount))
      }
    }

    //客户类型
    val customType: String = flatMap.getString("Z_RESERVED_CHAR5")
    val orderId: String = flatMap.getString("ORDER_ID")
    if (!CommonUtils.isNullOrEmpty(customType) && !CommonUtils.isNullOrEmpty(orderId)) {
      val jedisCluster: JedisCluster = EnvUtil.jedisCluster(env)
      val typeKey2redis: String = s"dataPlatform.RiskOrderType.${orderId}"
      if ("N".equals(customType)) {
        result.+=(((Constant.Flag.RISK_CUSTOMER_DISTRIBUTION, "new.count", dayStr), 1l))
        jedisCluster.set(typeKey2redis, "new")
      } else if ("J1".equals(customType)) {
        result.+=(((Constant.Flag.RISK_CUSTOMER_DISTRIBUTION, "settle.one.count", dayStr), 1l))
        jedisCluster.set(typeKey2redis, "settle.one")
      } else if ("J2".equals(customType)) {
        result.+=(((Constant.Flag.RISK_CUSTOMER_DISTRIBUTION, "settle.two.count", dayStr), 1l))
        jedisCluster.set(typeKey2redis, "settle.two")
      } else if ("Z1".equals(customType)) {
        result.+=(((Constant.Flag.RISK_CUSTOMER_DISTRIBUTION, "transit.one.count", dayStr), 1l))
        jedisCluster.set(typeKey2redis, "transit.one")
      } else if ("Z2".equals(customType)) {
        result.+=(((Constant.Flag.RISK_CUSTOMER_DISTRIBUTION, "transit.two.count", dayStr), 1l))
        jedisCluster.set(typeKey2redis, "transit.two")
      }
      //该类型顶多保存5天
      jedisCluster.expire(typeKey2redis, 432000)
    }

    //是否目标客户
    val target_flag: String =
      if ("Y".equals(flatMap.getString("I_IS_TNH_TARUSER")) ||
        "Y".equals(flatMap.getString("I_IS_YFQ_TARUSER")) ||
        "Y".equals(flatMap.getString("I_IS_POS_TARUSER"))) "targetCustomer"
      else "unTargetCustomer"
    result.+=(((Constant.Flag.RISK_CUSTOMER_TYPE_COUNT, target_flag, dayStr), 1l))

    //历史客户
    val apply_count1: Long = if (null == flatMap.getLong("C_HISAPPLY_VAR011")) 0l else flatMap.getLong("C_HISAPPLY_VAR011")
    val apply_count2: Long = if (null == flatMap.getLong("C_HISAPPLY_VAR001")) 0l else flatMap.getLong("C_HISAPPLY_VAR001")
    val history_flag = if ((apply_count1 + apply_count2) > 0) "historyCustomer" else "newCustomer"
    result.+=(((Constant.Flag.RISK_CUSTOMER_TYPE_COUNT, history_flag, dayStr), 1l))

    //同盾分数
    val k_score = flatMap.getInteger("K_FINAL_SCORE")
    if (null != k_score) {
      if (0 == k_score) {
        result.+=(((Constant.Flag.RISK_K_SCORE, "0", dayStr), 1l))
      } else if (k_score > 0 && k_score < 50) {
        result.+=(((Constant.Flag.RISK_K_SCORE, "0to50", dayStr), 1l))
      } else if (k_score >= 50) {
        result.+=(((Constant.Flag.RISK_K_SCORE, "50+", dayStr), 1l))
      }
    }

  }

  def writeRiskLocation(result: ArrayBuffer[((String, String, String), Long)], dayStr: String, flatMap: JSONObject): Any = {
    //A_APCR_NAMES 居住地址
    val city_name_str: String = flatMap.getString("A_APCR_NAMES")
    if (!CommonUtils.isNullOrEmpty(city_name_str)) {
      val words: Array[String] = city_name_str.split("\\|")
      if (words.length == 3) {
        val province: String = words(0).replace("省", "")
        val city: String = words(1).replace("市", "")
        result.+=(((Constant.Flag.RISK_PROVINCE_COUNT, province, dayStr), 1l))
        result.+=(((Constant.Flag.RISK_CITY_COUNT, city, dayStr), 1l))
      }
    }
  }


  /**
    * 对入参进行校验
    *
    * @param args
    */
  def assertArgs(args: Array[String]): Unit = {
    if (args.length < 5) {
      logError(
        s"""
           |Usage: DirectKafkaWordCount <envFlag> <topics> <groupid>  <seconds> <ckdir> <isClean>
           |  <EnvFlag> 标识环境种类
           |  <topics> 标识消费的主题
           |  <groupid> kafka消费者的组
           |  <seconds> 每一批次的时间
           |  <ckdir> checkpoint 路径
           |  <isClean> 是否需要清理历史记录,非必填，默认为false
        """.stripMargin)
      System.exit(1)
    }
  }

  def getProvince(flatMap: JSONObject, lbsProvince: String): String = {
    if (CommonUtils.isNullOrEmpty(lbsProvince)) {
      val belongProvince: String = flatMap.getString("belongProvince")
      if (CommonUtils.isNullOrEmpty(belongProvince)) {
        "default"
      } else {
        belongProvince
      }
    } else {
      lbsProvince
    }
  }

  def getCityInfo(flatMap: JSONObject, lbs: String): String = {
    if (CommonUtils.isNullOrEmpty(lbs)) {
      var beLongCity = flatMap.getString("belongCity")
      if (CommonUtils.isNullOrEmpty(beLongCity)) {
        "default"
      } else {
        beLongCity.replace("市", "")
      }
    } else {
      lbs.replace("市", "")
    }
  }

}

