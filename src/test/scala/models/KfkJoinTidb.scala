package models

import java.util.Arrays

import com.spark.test.KafkaProperties
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

object KfkJoinTidbbg {
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


  //
  /**
   * @fun 具体业务逻辑实现
   * @author jwp
   * @date 2021/09/05
   */

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


    broadcast.value.createOrReplaceTempView("project")


    var s_bookingsDs = ds.transform(rdd => {

      val df = rdd.map(w => {
        val jsonObject = JsonUtils.gson(w.value())
        val dt = jsonObject.get("data").getAsJsonArray.iterator()


        val s_bookings = dt.map(item => {
          val dt = item.getAsJsonObject

          S_booking(jsonObject.get("database").toString, jsonObject.get("table").toString, jsonObject.get("type").toString,
            dt.get("BookingGUID").toString, dt.get("BookingGUID").toString, dt.get("BookingGUID").toString,
            dt.get("BookingGUID").toString, dt.get("BookingGUID").toString, dt.get("BookingGUID").toString,
            dt.get("BookingGUID").toString, dt.get("BookingGUID").toString, dt.get("BookingGUID").toString
            , dt.get("CreatedTime").toString)
        })

        //        s_bookings.toStream.toDF().createOrReplaceTempView("booking")
        //        sqlC.sql("select * from booking").show()

        s_bookings
      })
      df
    })


    s_bookingsDs.foreachRDD(rdd => {
      val sqlC = SparkUtils.getSQLContextInstance(rdd.sparkContext)
      import sqlC.implicits._
      rdd.map(x => x.asInstanceOf[List[S_booking]]).toDF().show()
    })


    s_bookingsDs.print()


    ssc.start()
    ssc.awaitTermination()
  }

  //end run

}
