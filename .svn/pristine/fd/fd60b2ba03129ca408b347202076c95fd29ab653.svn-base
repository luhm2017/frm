package com.lakala.finance.stream.screen.common

import com.lakala.finance.common.EnvUtil
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.spark.Logging

/**
  * Created by longxiaolei on 2017/2/24.
  */
object KafkaOffsetUtils extends Logging {

  private var zkClient: ZkClient = null

  def getZkCli(env: String) = {
    if (zkClient == null) {
      synchronized {
        if (zkClient == null) {
          zkClient = new ZkClient(EnvUtil.kafkaZk(env))
        }
      }
    }
    zkClient
  }

  def delKafkaOffsetOnZk(env: String, topic: String, group: String) = {
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    val consumerOffsetDir = topicDirs.consumerOffsetDir
    val zk: ZkClient = new ZkClient(EnvUtil.kafkaZk(env))
    try {
      val children: Seq[String] = ZkUtils.getChildren(zk, consumerOffsetDir)
      children.foreach(x => {
        ZkUtils.deletePath(zk, s"$consumerOffsetDir/$x")
      })
      ZkUtils.deletePath(zk, consumerOffsetDir)
    } catch {
      case ex: ZkNoNodeException =>
        //如果查询不到子节点，该用户没有消费或者主题不存在，不用继续处理
        logWarning(s"group:$group 没有 $topic 的偏移量")
      case ex: NullPointerException => {
        logWarning(s"zk路径 ${consumerOffsetDir} 不存在")
      }

    } finally {
      zkClient.close()
    }
  }
}
