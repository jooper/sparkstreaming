package com.spark.test

import java.util.Arrays

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.common.util.KafkaConfig
import org.apache.spark.core.{SparkKafkaContext, StreamingKafkaContext}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

import scala.collection.JavaConversions._

object StreamingKafkaContextTest {
  val brokers = KafkaProperties.BROKER_LIST
  PropertyConfigurator.configure("conf/log4j.properties")


  def main(args: Array[String]): Unit = {
    run
  }

  def run() {

    val sc = new SparkContext(
      new SparkConf()
        .setMaster("local[2]")
        .setAppName("Test")
        .set(SparkKafkaContext.MAX_RATE_PER_PARTITION, "1")
    )

    var kp = StreamingKafkaContext.getKafkaParam(brokers, KafkaProperties.GROUP_ID,
      "earliest", "EARLIEST")

    //如果需要使用ssl验证，需要设置一下四个参数
    //    kp.put(
    //      SparkKafkaContext.DRIVER_SSL_TRUSTSTORE_LOCATION,
    //      "/mnt/kafka-key/client.truststore.jks"
    //    )
    //    kp.put(
    //      SparkKafkaContext.DRIVER_SSL_KEYSTORE_LOCATION,
    //      "/mnt/kafka-key/client.keystore.jks"
    //    )
    //    kp.put(
    //      SparkKafkaContext.EXECUTOR_SSL_TRUSTSTORE_LOCATION,
    //      "client.truststore.jks"
    //    )
    //    kp.put(
    //      SparkKafkaContext.EXECUTOR_SSL_KEYSTORE_LOCATION,
    //      "client.keystore.jks"
    //    )

    val ssc = new StreamingKafkaContext(kp.toMap, sc, Seconds(5))
    val consumOffset = getConsumerOffset(kp.toMap).foreach(println)
    val topics = Set(KafkaProperties.TOPIC)
    val ds = ssc.createDirectStream[String, String](topics)
    ds.foreachRDD { rdd =>
      println("COUNT : ", rdd.count)
      rdd.foreach(println)
      //使用自带的offset管理
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      //使用zookeeper来管理offset
      ssc.updateRDDOffsets(KafkaProperties.GROUP_ID, rdd)
    }
    ssc.start()
    ssc.awaitTermination()
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
  def initJobConf(conf: KafkaConfig) {
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
}
