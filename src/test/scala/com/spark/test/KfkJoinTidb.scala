package com.spark.test

import java.util.Arrays

import models.LhMessage
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.common.util.KafkaConfig
import org.apache.spark.core.StreamingKafkaContext
import org.apache.spark.rdbms.RdbmsUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.apache.spark.utils.{JsonUtils, SparkUtils}

import scala.collection.JavaConversions._

object KfkJoinTidb {
  val brokers = KafkaProperties.BROKER_LIST
  PropertyConfigurator.configure("conf/log4j.properties")

  def main(args: Array[String]): Unit = {
    run
  }

  /**
   * @func 提交offset。
   * @auto jwp
   * @date 2021/09/03
   */
  private def CommitOffset(ssc: StreamingKafkaContext, ds: InputDStream[ConsumerRecord[String, String]], rdd: RDD[ConsumerRecord[String, String]]) = {
    //使用自带的offset管理
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    //使用zookeeper来管理offset
    ssc.updateRDDOffsets(KafkaProperties.GROUP_ID, rdd)

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


  def run() {
    val sc = SparkUtils.getScInstall("local[*]", "s_booking")

    //项目维表数据
    val dimPro: DataFrame = RdbmsUtils.getDataFromTable(sc, "p_project", "p_projectId", "p_projectId")
      .persist(StorageLevel.MEMORY_ONLY)
    val broadcast = sc.broadcast(dimPro)


    var kp = StreamingKafkaContext.getKafkaParam(brokers, KafkaProperties.GROUP_ID,
      "EARLIEST", "EARLIEST")

    val ssc = new StreamingKafkaContext(kp.toMap, sc, Seconds(10))

    //输出最后一次消费的offset
    getConsumerOffset(kp.toMap).foreach(item => println("上次消费的topic：%s，offset：%s".format(item._1, item._2)))


    val ds: InputDStream[ConsumerRecord[String, String]] = ssc.createDirectStream[String, String](Set(KafkaProperties.TOPIC))


    //        ds.foreachRDD { rdd =>
    //          println("COUNT : ", rdd.count)
    //          rdd.foreach(println)
    //          rdd.map(_.value()).writeToKafka(producerConfig(brokers), transformFunc(outTopic, _))
    //          CommitOffset(ssc, ds, rdd)
    //        }


    broadcast.value.createOrReplaceTempView("project")


    ds.transform(rdd => {
      val sqlC = SparkUtils.getSQLContextInstance(rdd.sparkContext)
      import sqlC.implicits._
      val df = rdd.map(w => {
        val jsonObject = JsonUtils.gson(w.value())
        LhMessage(jsonObject.get("database").toString, jsonObject.get("table").toString, jsonObject.get("type").toString)

      }).toDF()

      df.rdd
      //
      //
      //      df.createOrReplaceTempView("booking")
      //      val sql = "select * from booking bk left join project pr on 1=1"
      //      val result: DataFrame = sqlC.sql(sql)
      //      result.rdd
    }).print()


    //    ds.foreachRDD { rdd =>
    //      val sqlC = SparkUtils.getSQLContextInstance(rdd.sparkContext)
    //      //      sqlC.sql("select * from project").show()
    //
    //
    //      rdd.foreach(item => {
    //        val jsonObject = JsonUtils.gson(item.value())
    //        LhMessage(jsonObject.get("database").toString, jsonObject.get("table").toString, jsonObject.get("type").toString)
    //
    //
    //        println(LhMessage)
    //
    //        //        jsonObject.get("data").getAsJsonArray.map(item => {
    //        //          println(item.getAsJsonObject.get("BookingGUID").toString)
    //        //        })
    //
    //      })
    //    }


    ssc.start()
    ssc.awaitTermination()
  }

  //end run

}
