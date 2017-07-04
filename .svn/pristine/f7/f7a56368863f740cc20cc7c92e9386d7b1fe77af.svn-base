package com.lakala.finance.stream.screen.common

import java.io.{Closeable, PrintWriter, StringWriter}
import java.util
import java.util.Map.Entry

import com.lakala.finance.common.EnvUtil
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.OffsetRange

/**
  * Created by longxiaolei on 2017/2/20.
  */
object CommonUtils extends Logging {
  def isNullOrEmpty(str: String): Boolean = {
    return null == str || "".equals(str.trim)
  }

  def outPutDstream[T](env_flag: String, dStream: DStream[T], outFunc: Iterator[T] => Unit) = {
    dStream.foreachRDD((rdd: RDD[T]) => {
      rdd.foreachPartition((iterator: Iterator[T]) => {
        //因为加载操作只在driver端执行，会导致分区中没有加载环境信息，所以需要再各个分区中再加载一起环境配置
        outFunc(iterator)
      })
    })
  }

  def getExceptionInfo(exception: Exception): String = {
    var out: StringWriter = null
    var pw: PrintWriter = null
    try {
      out = new StringWriter();
      pw = new PrintWriter(out)
      exception.printStackTrace(pw)
      out.toString
    } finally {
      closeStream(out)
      closeStream(pw)
    }
  }



  //关闭各种流，避免写if判空的逻辑
  def closeStream(stream: Closeable) = {
    if (stream != null) {
      stream.close()
    }
  }

  def logException(ex: Exception): Unit = {
    val errorInf: String = getExceptionInfo(ex)
    logError(errorInf)
  }

  private val offsetPathPrefix = "dataPlatform.KafkaOffsets"
  def writeOffsets2Redis(envFlag: String, group: String, offsetRanges: Array[OffsetRange]): Unit = {
    //完成计算任务后将相应的偏移量刷入到redis中
    val topic_map: util.HashMap[String, util.HashMap[String, String]] = new util.HashMap[String, util.HashMap[String, String]]()
    for (o <- offsetRanges) {
      //将偏移量信息入库到redis中
      val topic: String = o.topic
      val partition: Int = o.partition
      val offset: Long = o.untilOffset
      var result: util.HashMap[String, String] = topic_map.get(topic)
      if (result == null) {
        result = new util.HashMap[String, String]()
        topic_map.put(topic, result)
      }
      result.put(partition.toString, offset.toString)
    }

    val iterator: util.Iterator[Entry[String, util.HashMap[String, String]]] = topic_map.entrySet().iterator()
    val jedis = EnvUtil.jedisCluster(envFlag)
    while (iterator.hasNext) {
      val entry: Entry[String, util.HashMap[String, String]] = iterator.next()
      val rediskey: String = s"${offsetPathPrefix}.${group}.${entry.getKey}"
      jedis.del(rediskey)
      jedis.hmset(rediskey, entry.getValue)
    }
  }

  def main(args: Array[String]) {
    print(1 / 5)
  }
}
