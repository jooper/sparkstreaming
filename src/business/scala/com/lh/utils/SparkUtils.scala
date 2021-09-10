package com.lh.utils

import java.util.Arrays

import com.spark.test.KafkaProperties
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.spark.common.util.KafkaConfig
import org.apache.spark.core.{SparkKafkaContext, StreamingKafkaContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object SparkUtils {
  @transient private var sparkInstance: SparkContext = _
  @transient private var sqlInstance: SQLContext = _
  @transient private var sccInstance: StreamingKafkaContext = _

  def getSQLContextInstance(sparkContext: SparkContext, isSingle: Boolean = true): SQLContext = {
    if (isSingle) {
      if (sqlInstance == null) {
        sqlInstance = new SQLContext(sparkContext)
      }
      sqlInstance
    }
    else {
      new SQLContext(sparkContext)
    }
  }

  def getSQLContextInstance(): SQLContext = {
    if (sqlInstance == null) {
      getScInstall("local[*]", "default")
      sqlInstance = new SQLContext(sparkInstance)
    }
    sqlInstance
  }

  def getScInstall(master: String, appName: String): SparkContext = {
    if (sparkInstance == null) {
      sparkInstance = new SparkContext(
        new SparkConf()
          .setMaster(master)
          .setAppName(appName)
          .set(SparkKafkaContext.MAX_RATE_PER_PARTITION, "1")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("mergeSchema", "true")
          .set("spark.driver.host", "localhost")
          .set("spark.streaming.backpressure.initialRate", "30") //初始速率500条/s
          .set("spark.streaming.backpressure.enabled", "true") //开启压背
          .set("spark.streaming.kafka.maxRatePerPartition", "5000") //最大速度不超过5000条
          .set("per.partition.offsetrange.step", "1000")
          .set("per.partition.offsetrange.threshold", "1000")
          .set("enable.auto.repartion", "false")
        //    sc.getConf.set("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        //    sc.getConf.set("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      )
    }
    sparkInstance
  }

  def getKfkSccInstall(master: String, appName: String, brokers: String, groupId: String, checkpoinDir: String,
                       consumerFrom: String, errorFrom: String, windowSizeSecend: Long) = {
    if (sccInstance == null) {
      val sc = SparkUtils.getScInstall(master, appName)
      sc.setCheckpointDir("%S/sparkCheckPoint/%s".format(KfkProperties.HDFS, checkpoinDir))
      val kp = StreamingKafkaContext.getKafkaParam(brokers, groupId, consumerFrom, errorFrom)
      sccInstance = new StreamingKafkaContext(kp.toMap, sc, Seconds(windowSizeSecend))
    }
    sccInstance
  }


  /**
   * @func 提交offset。
   * @auto jwp
   * @date 2021/09/03
   */
  def CommitOffset(ssc: StreamingKafkaContext, ds: InputDStream[ConsumerRecord[String, String]], rdd: RDD[ConsumerRecord[String, String]]) = {
    //使用自带的offset管理
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    //使用zookeeper来管理offset
    ssc.updateRDDOffsets(KafkaProperties.GROUP_ID, rdd)
    //    println(rdd.partitions.foreach("partition:%s".format(_)))
    println("commited offset:" + offsetRanges)
  }

  /**
   * @func 获取上次消费偏移量。
   */
  def getConsumerOffset(kp: Map[String, Object]) = {
    val consumer = new KafkaConsumer[String, String](kp)
    consumer.subscribe(Arrays.asList(KafkaProperties.TOPIC)); //订阅topic
    consumer.poll(0)
    val parts = consumer.assignment() //获取topic等信息
    val re = parts.map { ps =>
      ps -> consumer.position(ps)
    }
    consumer.pause(parts)
    re
  }


  /**
   * 初始化配置文件
   */
  def initJobConf(conf: KafkaConfig, brokers: String) {
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> KafkaProperties.GROUP_ID,
      "kafka.last.consum" -> "last"
    )
    val topics = Set(KafkaProperties.TOPIC)
    conf.setKafkaParams(kp)
    conf.setTopics(topics)
  }


  def setHdfsUser(uname: String): Unit = {
    System.setProperty("HADOOP_USER_NAME", uname)
  }
}
