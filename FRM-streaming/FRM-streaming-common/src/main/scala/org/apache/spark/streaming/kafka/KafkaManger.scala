package org.apache.spark.streaming.kafka

import java.util
import java.util.Map.Entry

import com.lakala.finance.common.EnvUtil
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.{Logging, SparkException}
import redis.clients.jedis.JedisCluster
import redis.clients.jedis.exceptions.JedisException

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  *
  * @param kafkaParams 必填 group.id 、 metadata.broker.list 信息, 选填auto.offset.reset（smallest/largest）
  */
class KafkaManger(val kafkaParams: Map[String, String], env:String) extends Logging {
  protected val kc = new KafkaCluster(kafkaParams)

  private val OFFSET_PATH_PREFIX = "dataPlatform.KafkaOffsets"

  private val jedis: JedisCluster = EnvUtil.jedisCluster(env)

  import KafkaCluster.LeaderOffset

  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag](
                                                                                                              ssc: StreamingContext, topics: Set[String]): InputDStream[(K, V)] = {
    val groupId = kafkaParams.get("group.id").get

    //现根据实际情况刷新redis中的偏移量信息
    setOrUpdateOffsetInRedis(topics, groupId)

    //从redis中读取偏移量并消费kafka信息
    val messages = {
      val partitionsE = kc.getPartitions(topics)
      if (partitionsE.isLeft)
        throw new SparkException(s"获取topic的分区信息失败: ${partitionsE.left.get}")

      val partitions = partitionsE.right.get
      val consumerOffsetsE = getOffsetsInRedis(topics, groupId)

      if (consumerOffsetsE.isLeft)
        throw new SparkException(s"获取comsumer偏移量信息失败: ${consumerOffsetsE.left.get}")

      val consumerOffsets = consumerOffsetsE.right.get
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
        ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message))
    }
    messages
  }


  /**
    * 创建数据流前，根据实际消费情况更新消费offsets
    *
    * @param topics
    * @param group
    */
  def setOrUpdateOffsetInRedis(topics: Set[String], group: String): Unit = {
    topics.foreach(topic => {
      var hasConsumed = false
      var partitionsE: Either[Err, Set[TopicAndPartition]] = getPartitions(Set(topic))
      if (partitionsE.isLeft) {
        logWarning(s"第一次获取 topic: ${topic} 的分区信息失败，可能是该topic不存在导致，等待topic自动构建，再次尝试再次获取")
        Thread.sleep(1000)
        partitionsE = getPartitions(Set(topic))
        if (partitionsE.isLeft)
          throw new SparkException(s"获取kafka分区信息失败: ${partitionsE.left.get}")
      }

      val partitions: Set[TopicAndPartition] = partitionsE.right.get

      val consumerOffsetsE: Either[Err, Map[TopicAndPartition, Long]] = getOffsetsInRedis(topic, group)
      if (consumerOffsetsE.isLeft)
        throw new SparkException(s"redis集群异常导致获取偏移量信息失败 : ${consumerOffsetsE.left.get}")
      val consumerOffsets: Map[TopicAndPartition, Long] = consumerOffsetsE.right.get
      if (consumerOffsets.size > 0)
        hasConsumed = true

      if (hasConsumed) {
        /**
          * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
          * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
          * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          */
        val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if (earliestLeaderOffsetsE.isLeft)
          throw new SparkException(s"获取earliet offset失败: ${earliestLeaderOffsetsE.left.get}")

        val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
        // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = Map()
        consumerOffsets.foreach({ case (tp, n) =>
          val earliestLeaderOffset: LeaderOffset = earliestLeaderOffsets(tp)
          val earliestOffset = earliestLeaderOffset.offset
          if (n < earliestOffset) {
            logWarning("consumer group:" + group + ",topic:" + tp.topic + ",partition:" + tp.partition +
              " offsets已经过时，更新为" + earliestOffset)
            leaderOffsets += (tp -> earliestLeaderOffset)
          }
        })
        setOffset2Redis(leaderOffsets, group)

      } else {
        //没有消费过
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        val reset: String = kafkaParams.getOrElse("auto.offset.reset", "largest")
        if ("smallest".equals(reset)) {
          val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"获取earliet offset失败: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        } else {
          val leaderOffsetsE = kc.getLatestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"获取latest offset失败: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        }
        setOffset2Redis(leaderOffsets, group)
      }
    })
  }

  def setOffset2Redis(leaderOffsets: Map[TopicAndPartition, LeaderOffset], group: String) = {
    val topic2Offsets: util.HashMap[String, util.HashMap[String, String]] = new util.HashMap[String, util.HashMap[String, String]]()
    leaderOffsets.foreach(x => {
      val topicAndPartition: TopicAndPartition = x._1
      val offset: Long = x._2.offset
      var offsets: util.HashMap[String, String] = topic2Offsets.get(topicAndPartition.topic)
      if (offsets == null) offsets = new util.HashMap[String, String]()
      offsets.put(topicAndPartition.partition.toString, offset.toString)
      topic2Offsets.put(topicAndPartition.topic, offsets)
    })

    val it: util.Iterator[Entry[String, util.HashMap[String, String]]] = topic2Offsets.entrySet().iterator()
    while (it.hasNext) {
      val entry: Entry[String, util.HashMap[String, String]] = it.next()
      val offset2redis: util.HashMap[String, String] = entry.getValue
      val topic: String = entry.getKey
      val offsetKey = s"${OFFSET_PATH_PREFIX}.${group}.${topic}"
      jedis.del(offsetKey)
      jedis.hmset(offsetKey, offset2redis)
    }

  }

  /**
    * 查询topics上的所有的偏移量信息，一定能要求所有的topic的偏移量存在才能正常查询
    *
    * @param topics
    * @param group
    * @return
    */
  def getOffsetsInRedis(topics: Set[String], group: String): Either[Err, Map[TopicAndPartition, Long]] = {
    var result: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
    val errs = new Err
    topics.foreach(topic => {
      try {
        val offsetKey = s"${OFFSET_PATH_PREFIX}.${group}.${topic}"
        import scala.collection.JavaConverters._
        val offsetInRedis: mutable.Map[String, String] = jedis.hgetAll(offsetKey).asScala
        offsetInRedis.foreach(x => {
          val partition: Int = x._1.toInt
          val offset: Long = x._2.toLong
          result = result.+((TopicAndPartition(topic, partition), offset))
        })
      } catch {
        case ex: JedisException =>
          errs.append(ex)
        case ex: Exception =>
          errs.append(ex)
      }
    })
    if (errs.size > 0) {
      return Left(errs)
    }
    Right(result)
  }

  /**
    * 根据topic和group查询redis中记录的偏移量信息
    *
    * @param topic
    * @param group
    * @return
    */
  def getOffsetsInRedis(topic: String, group: String): Either[Err, Map[TopicAndPartition, Long]] = {
    var result: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
    val errs = new Err
    val offsetKey = s"${OFFSET_PATH_PREFIX}.${group}.${topic}"
    try {
      import scala.collection.JavaConverters._
      val offsetInRedis: mutable.Map[String, String] = jedis.hgetAll(offsetKey).asScala
      offsetInRedis.foreach(x => {
        val partition: Int = x._1.toInt
        val offset: Long = x._2.toLong
        result = result.+((TopicAndPartition(topic, partition), offset))
      })
    } catch {
      case ex: JedisException =>
        errs.append(ex)
      case ex: Exception =>
        errs.append(ex)
    }
    if (errs.size > 0) {
      return Left(errs)
    }
    Right(result)
  }

  /**
    * 根据topoic获取分区信息
    *
    * @param topics
    * @return
    */
  def getPartitions(topics: Set[String]): Either[Err, Set[TopicAndPartition]] = {
    kc.getPartitions(topics)
  }

  /**
    * 获取每个分区对应的最大的leader偏移量
    *
    * @param topicAndPartitions Set[TopicAndPartition]
    * @return
    */
  def getLatestLeaderOffsets(topicAndPartitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, LeaderOffset]] = {
    kc.getLatestLeaderOffsets(topicAndPartitions)
  }

  /**
    * 获取每个分区对应的最早的偏移量
    *
    * @param topicAndPartitions Set[TopicAndPartition]
    * @return
    */
  def getEarliestLeaderOffsets(topicAndPartitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, LeaderOffset]] = {
    kc.getEarliestLeaderOffsets(topicAndPartitions)
  }

  /**
    * 清理redis上该topic的偏移量位置
    *
    * @param topics
    */
  def delOffsetInRedis(topics: Set[String], group: String): Unit = {
    topics.foreach(topic => {
      val offsetKey: String = s"${OFFSET_PATH_PREFIX}.${group}.${topic}"
      jedis.del(offsetKey)
    })
  }
}
