package com.lh.utils

import java.util.Arrays


import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.spark.common.util.KafkaConfig
import org.apache.spark.core.{SparkKafkaContext, StreamingKafkaContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
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
          .set("spark.driver.host", "localhost")
          .set("spark.streaming.backpressure.enabled", "true") //开启反压
          .set("spark.streaming.backpressure.pid.minRate", "1") //最小摄入条数控制
          //控制每秒读取Kafka每个Partition最大消息数(500*3*10=15000)，若Streaming批次为10秒，topic最大分区为3，则每批次最大接收消息数为15000
          // direct模式，限制每秒钟从topic的每个partition最多消费的消息条数
          .set("spark.streaming.kafka.maxRatePerPartition", "1200")
          //单位：毫秒，设置从Kafka拉取数据的超时时间，超时则抛出异常重新启动一个task，等待拿数据的时间
          .set("spark.streaming.kafka.consumer.poll.ms", "1200000000")
          //开启推测，防止某节点网络波动或数据倾斜导致处理时间拉长(推测会导致无数据处理的批次，也消耗与上一批次相同的执行时间，但不会超过批次最大时间，可能导致整体处理速度降低)
          .set("spark.speculation", "true")


          //是否开启自动重分区
          //.set("enable.auto.repartion", "false")
          //避免不必要的重分区操作，增加个阈值，只有该批次要消费的kafka的分区内数据大于该阈值才进行拆分
          .set("per.partition.offsetrange.threshold", "300")
          //拆分后，每个kafkardd 的分区数据量。
          .set("per.partition.after.partition.size", "100")
          .set("mergeSchema", "true")
          .set("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          .set("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          .set("per.partition.offsetrange.step", "1000")
          .set("per.partition.offsetrange.threshold", "1000")
          .set("spark.streaming.backpressure.initialRate", "30") //初始速率500条/s  只适用于receive模式

      )
    }
    sparkInstance
  }

  def getKfkSccInstall(master: String, appName: String, brokers: String, groupId: String, checkpoinDir: String,
                       consumerFrom: String, errorFrom: String, windowSizeSecend: Long) = {
    if (sccInstance == null) {
      val sc = SparkUtils.getScInstall(master, appName)
      sc.setCheckpointDir("%s/sparkCheckPoint/%s".format(ConfigUtils.HDFS, checkpoinDir))
      val kp = StreamingKafkaContext.getKafkaParam(brokers, groupId, consumerFrom, errorFrom)
      //输出最后一次消费的offset
      SparkUtils.getConsumerOffset(kp.toMap).foreach(item => println("上次消费的topic：%s，offset：%s".format(item._1, item._2)))
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
    ssc.updateRDDOffsets(ConfigUtils.GROUP_ID, rdd)
    //    println(rdd.partitions.foreach("partition:%s".format(_)))
    println("commited offset:" + offsetRanges)
  }

  /**
   * @func 提交offset。
   * @auto jwp
   * @date 2021/09/14
   */
  def CommitRddOffset(ds: InputDStream[ConsumerRecord[String, String]]) = {
    ds.foreachRDD(rd => {
      //offsetRanges只有直接对接kafka流的第一个rdd才能获取到相关offset信息，这里先存储信息，后续rdd经过转化后就无法获取相关信息
      val offsetRanges = rd.asInstanceOf[HasOffsetRanges].offsetRanges
      ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

  }

  /**
   * @func 获取上次消费偏移量。
   */
  def getConsumerOffset(kp: Map[String, Object]) = {
    val consumer = new KafkaConsumer[String, String](kp)
    consumer.subscribe(Arrays.asList(ConfigUtils.TOPIC)); //订阅topic
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
      "group.id" -> ConfigUtils.GROUP_ID,
      "kafka.last.consum" -> "last"
    )
    val topics = Set(ConfigUtils.TOPIC)
    conf.setKafkaParams(kp)
    conf.setTopics(topics)
  }

  /**
   * @fun 保存rdd信息到指定的topic中
   * @author jwp
   * @date 2021-09-11
   **/
  def sinkDfToKfk(resultDf: DataFrame, topic: String) = {
    resultDf
      .write
      .mode("append") //append 追加  overwrite覆盖   ignore忽略  error报错,写到kafka只能是append
      .format("kafka")
      .option("ignoreNullFields", "false")
      .option("kafka.bootstrap.servers", ConfigUtils.BROKER_LIST)
      .option("topic", topic)
      .save()
  }

  /**
   * @fun 设置访问hdfs的用户
   * @author jwp
   * @date 2021-09-11
   */
  def setHdfsUser(uname: String): Unit = {
    System.setProperty("HADOOP_USER_NAME", uname)
  }
}
