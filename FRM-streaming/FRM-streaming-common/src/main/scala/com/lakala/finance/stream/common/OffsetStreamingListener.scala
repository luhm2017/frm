package com.lakala.finance.stream.common

import java.util
import java.util.Map.Entry

import com.lakala.finance.common.EnvUtil
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

import scala.collection.mutable

/**
  * Created by longxiaolei on 2017/3/29.
  */
class OffsetStreamingListener(env: String, kafkaParams: Map[String, String], offsetRanges: mutable.Map[Time, Array[OffsetRange]]) extends StreamingListener {
  private val offsetPathPrefix = "dataPlatform.KafkaOffsets"
  private val jedis = EnvUtil.jedisCluster(env)

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val curOffsetRange = offsetRanges.remove(batchCompleted.batchInfo.batchTime).get
    //store offset
    val group = kafkaParams.get("group.id").get
    val topic_map: util.HashMap[String, util.HashMap[String, String]] = new util.HashMap[String, util.HashMap[String, String]]()

    curOffsetRange.foreach(x => {
      val topic: String = x.topic
      val partition: Int = x.partition
      val offset: Long = x.fromOffset
      var result: util.HashMap[String, String] = topic_map.get(topic)
      if (result == null) {
        result = new util.HashMap[String, String]()
        topic_map.put(topic, result)
      }
      result.put(partition.toString, offset.toString)
    })

    val iterator: util.Iterator[Entry[String, util.HashMap[String, String]]] = topic_map.entrySet().iterator()
    while (iterator.hasNext) {
      val entry: Entry[String, util.HashMap[String, String]] = iterator.next()
      val rediskey: String = s"${offsetPathPrefix}.${group}.${entry.getKey}"
      jedis.del(rediskey)
      jedis.hmset(rediskey, entry.getValue)
    }
  }
}

