package com.lakala.finance.common

import java.util
import java.util.Map.Entry

import redis.clients.jedis.JedisCluster

/**
  * Created by longxiaolei on 2017/3/31.
  */
object GetRedisInfo {
  def main(args: Array[String]) {
    val jedis: JedisCluster = EnvUtil.jedisCluster(args(0))

    val keyname: String = args(1)
    val type_name: String = args(2)

    if ("string".equals(type_name)) {
      println(jedis.get(keyname))
    } else if ("set".equals(type_name)) {
      val smembers: util.Set[String] = jedis.smembers(keyname)
      val it: util.Iterator[String] = smembers.iterator()
      while (it.hasNext) {
        val next: String = it.next()
        println(next)
      }
    } else if ("hset".equals(type_name)) {
      val all: util.Map[String, String] = jedis.hgetAll(keyname)
      val entrySet: util.Set[Entry[String, String]] = all.entrySet()
      val it: util.Iterator[Entry[String, String]] = entrySet.iterator()
      while (it.hasNext) {
        val entry: Entry[String, String] = it.next()
        println(entry.getKey + "," + entry.getValue)
      }
    }
  }

}
