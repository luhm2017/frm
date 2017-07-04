package com.lakala.finance.common

import java.io.InputStream
import java.util

import com.alibaba.druid.pool.DruidDataSource
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.io.Source


object EnvUtil {
  private val is: InputStream = getClass.getClassLoader.getResourceAsStream("env.json")
  private val json: String = Source.fromInputStream(is).mkString
  private val envInfo: JSONObject = JSON.parseObject(json)

  def getComponetInfoObj(env: String, componetName: String) = {
    envInfo.getJSONObject(env).getJSONObject(componetName)
  }

  def getComponetInfoArray(env: String, componetName: String) = {
    envInfo.getJSONObject(env).getJSONArray(componetName)
  }


  private var kafkaBrokers: String = _
  /**
    * 获取kafka的brokers信息
    *
    * @param env 环境标识
    * @return
    */
  def kafkaBrokers(env: String):String = {
    if (kafkaBrokers == null) {
      synchronized {
        if (kafkaBrokers == null) {
          kafkaBrokers = envInfo.getJSONObject(env).getJSONObject("kafka").getString("brokers")
        }
      }
    }
    kafkaBrokers
  }

  private var kafkaZK :String = _
  def kafkaZk(env:String):String ={
    if (kafkaZK == null) {
      synchronized {
        if (kafkaZK == null) {
          kafkaZK = envInfo.getJSONObject(env).getJSONObject("kafka").getString("zk")
        }
      }
    }
    kafkaZK
  }


  private var cluster: JedisCluster = _
  /**
    * 获取redis的信息
    *
    * @param env 环境信息
    * @return
    */
  def jedisCluster(env: String): JedisCluster = {
    if (cluster == null) {
      synchronized {
        if (cluster == null) {
          val nodis: JSONArray = EnvUtil.getComponetInfoObj(env, "redis").getJSONArray("nodes")
          val size: Int = nodis.size()
          if (size <= 0) {
            throw new RuntimeException("获取配置信息失败，请检查env.json文件")
          }
          val jedisClusterNodes: util.HashSet[HostAndPort] = new util.HashSet[HostAndPort](size)
          val it = nodis.iterator()
          while (it.hasNext) {
            val nodeObject = it.next().asInstanceOf[JSONObject]
            val hostAndPort: HostAndPort = new HostAndPort(nodeObject.getString("host"), nodeObject.getIntValue("port"))
            jedisClusterNodes.add(hostAndPort)
          }
          cluster = new JedisCluster(jedisClusterNodes)
        }
      }
    }
    cluster
  }

  private var mysqlDataSource: DruidDataSource = _
  /**
    * 根据环境信息获取数据库连接
    * @param env
    * @return
    */
  def mysqlDS(env:String): DruidDataSource = {
    if (mysqlDataSource == null) {
      synchronized {
        if (mysqlDataSource == null) {
          mysqlDataSource = new DruidDataSource
          mysqlDataSource.setDriverClassName("com.mysql.jdbc.Driver")
          mysqlDataSource.setUrl(getComponetInfoObj(env,"mysql").getString("url"))
          mysqlDataSource.setUsername(getComponetInfoObj(env,"mysql").getString("username"))
          mysqlDataSource.setPassword(getComponetInfoObj(env,"mysql").getString("passwd"))
          mysqlDataSource.setInitialSize(5)
          mysqlDataSource.setMaxActive(10)
          //最多等待连接3秒钟
          mysqlDataSource.setMaxWait(3000)
          mysqlDataSource.setPoolPreparedStatements(true)
          mysqlDataSource.setMaxOpenPreparedStatements(10)
        }
      }
    }
    mysqlDataSource
  }


}
