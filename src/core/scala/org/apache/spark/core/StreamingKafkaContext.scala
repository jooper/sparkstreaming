package org.apache.spark.core

import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.common.util.KafkaConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.StreamingKafkaManager

import scala.reflect.ClassTag

/**
 * @author LMQ
 * @description 用于替代StreamingContext。但StreamingContext的很多功能没写，可以自己添加，或者直接拿StreamingContext使用
 * @description 此类主要是用于读取kafka数据， 创建 Dstream 。
 * @description 目前只提供createDirectStream的方式读取kafka
 */
class StreamingKafkaContext(var kp: Map[String, String]) {
  var streamingContext: StreamingContext = null
  lazy val skm = new StreamingKafkaManager(kp) //这个类似于KafkaCluster
  var sc: SparkContext = null

  def this(kp: Map[String, String], streamingContext: StreamingContext) {
    this(kp)
    this.streamingContext = streamingContext
    this.sc = streamingContext.sparkContext
  }

  def this(kp: Map[String, String], sc: SparkContext, batchDuration: Duration) {
    this(kp)
    this.sc = sc
    streamingContext = new StreamingContext(sc, batchDuration)
  }

  def start() {
    streamingContext.start()
  }

  def awaitTermination() {
    streamingContext.awaitTermination
  }

  /**
   * @author LMQ
   * @description 将当前的topic的偏移量更新至最新。（相当于丢掉未处理的数据）
   * @return lastestOffsets ：返回最新的offset
   */
  def updataOffsetToLastest(topics: Set[String], kp: Map[String, String]) = {
    val lastestOffsets = getLastOffset(topics)
    skm.updateConsumerOffsets(lastestOffsets)
    lastestOffsets
  }

  /**
   * @author LMQ
   * @description 获取最新的offset
   * @return lastestOffsets ：返回最新的offset
   */
  def getLastOffset(topics: Set[String]) = {
    skm
      .getLatestOffsets(topics)
  }

  /**
   * @author LMQ
   * @description 更新rdd的offset
   */
  def updateRDDOffsets[T](groupId: String, rdd: RDD[T]) {
    skm.updateRDDOffset(groupId, rdd)
  }

  /**
   * @author LMQ
   * @description 更新rdd的offset
   */
  def updateRDDOffsets[T](rdd: RDD[T]) {
    if (kp.contains("group.id")) {
      val groupid = kp.get("group.id").get
      skm.updateRDDOffset(groupid, rdd)
    } else println("No Group Id To UpdateRDDOffsets")
  }

  /**
   * @author LMQ
   * @description 获取rdd的offset
   */
  def getRDDOffsets[T](rdd: RDD[T]) = {
    skm.getRDDConsumerOffsets(rdd)
  }

  /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param topics     ：topic列表
   * @param fromOffset ： 读取数据的offset起点
   */
  def createDirectStream(
                          topics: Set[String],
                          fromOffset: Map[TopicAndPartition, Long]
                        ) = {
    skm
      .createDirectStream[String, String](streamingContext, topics, fromOffset)
  }

  /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param topics     ：topic列表
   * @param fromOffset ： 读取数据的offset起点
   */
  def createDirectStream[K: ClassTag, V: ClassTag](
                                                    topics: Set[String],
                                                    fromOffset: Map[TopicAndPartition, Long]
                                                  ): InputDStream[ConsumerRecord[K, V]] = {
    skm
      .createDirectStream[K, V](streamingContext, topics, fromOffset)
  }

  /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param topics ：topic列表
   */
  def createDirectStream[K: ClassTag, V: ClassTag](
                                                    topics: Set[String]
                                                  ): InputDStream[ConsumerRecord[K, V]] = {
    skm
      .createDirectStream[K, V](streamingContext, topics, null)
  }

  /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param topics ：topic列表
   */
  def createDirectStream(
                          topics: Set[String]
                        ): InputDStream[ConsumerRecord[String, String]] = {
    skm
      .createDirectStream[String, String](streamingContext, topics, null)
  }

  /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param conf       ： 包含kp和topics
   * @param fromOffset ： 读取数据的offset起点
   */
  def createDirectStream(
                          conf: KafkaConfig,
                          fromOffset: Map[TopicAndPartition, Long]
                        ) = {
    skm
      .createDirectStream[String, String](streamingContext, conf, fromOffset)
  }

  /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param conf       ： 包含kp和topics
   * @param fromOffset ： 读取数据的offset起点
   */
  def createDirectStream[K: ClassTag, V: ClassTag](
                                                    conf: KafkaConfig,
                                                    fromOffset: Map[TopicAndPartition, Long]
                                                  ) = {
    skm
      .createDirectStream[K, V](streamingContext, conf, fromOffset)
  }

  /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param conf ： 包含kp和topics
   */
  def createDirectStream[K: ClassTag, V: ClassTag](
                                                    conf: KafkaConfig
                                                  ): InputDStream[ConsumerRecord[K, V]] = {
    skm
      .createDirectStream[K, V](streamingContext, conf, null)
  }

  /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param conf ： 包含kp和topics
   */
  def createDirectStream(
                          conf: KafkaConfig
                        ) = {
    skm
      .createDirectStream[String, String](streamingContext, conf, null)
  }
}

object StreamingKafkaContext extends SparkKafkaConfsKey {}
