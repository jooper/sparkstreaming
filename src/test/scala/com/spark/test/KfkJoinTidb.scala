package models

import java.util.Arrays

import com.spark.test.{KafkaProperties, outTopic, producerConfig, transformFunc}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.common.util.KafkaConfig
import org.apache.spark.core.StreamingKafkaContext
import org.apache.spark.rdbms.RdbmsUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.utils.SparkUtils

import scala.collection.JavaConversions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.core.StreamingKafkaContext
import org.apache.spark.func.tool._
import kafka.serializer.StringDecoder

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


  //
  /**
   * @fun 具体业务逻辑实现
   * @author jwp
   * @date 2021/09/05
   */

  def run() {
    val sc = SparkUtils.getScInstall("local[*]", "s_booking")

    //项目维表数据
    val dimPro: DataFrame = RdbmsUtils.getDataFromTable(sc, "p_project", "p_projectId", "projName")
      .persist(StorageLevel.MEMORY_ONLY)
    val broadcast = sc.broadcast(dimPro)


    var kp = StreamingKafkaContext.getKafkaParam(brokers, KafkaProperties.GROUP_ID,
      "EARLIEST", "EARLIEST")

    val ssc = new StreamingKafkaContext(kp.toMap, sc, Seconds(10))

    //输出最后一次消费的offset
    getConsumerOffset(kp.toMap).foreach(item => println("上次消费的topic：%s，offset：%s".format(item._1, item._2)))


    val ds: InputDStream[ConsumerRecord[String, String]] = ssc.createDirectStream[String, String](Set(KafkaProperties.TOPIC))


    broadcast.value.createOrReplaceTempView("project")


    val elementDstream = ds.map(v => v.value).foreachRDD { rdd =>
      val sqlC = SparkUtils.getSQLContextInstance(rdd.sparkContext)

      sqlC.sql("select * from project").printSchema()

      val df = sqlC.read.schema(Schemas.sbookingSchema3).json(rdd)

      //      df.printSchema()
      df.createOrReplaceTempView("booking")


      val sqlStr =
        """
          |select
          |database,
          |table,
          |type,
          |BookingGUID, BgnDate,ProjGuid,Status,ProjNum,x_IsTPFCustomer,x_TPFCustomerTime,x_IsThirdCustomer,
          |x_ThirdCustomerTime,CreatedTime
          |from booking
          |lateral view explode(data.BookingGUID) exploded_names as BookingGUID
          |lateral view explode(data.BgnDate) exploded_colors as BgnDate
          |lateral view explode(data.ProjGuid) exploded_colors as ProjGuid
          |lateral view explode(data.Status) exploded_colors as Status
          |lateral view explode(data.ProjNum) exploded_colors as ProjNum
          |lateral view explode(data.x_IsTPFCustomer) exploded_colors as x_IsTPFCustomer
          |lateral view explode(data.x_TPFCustomerTime) exploded_colors as x_TPFCustomerTime
          |lateral view explode(data.x_IsThirdCustomer) exploded_colors as x_IsThirdCustomer
          |lateral view explode(data.x_ThirdCustomerTime) exploded_colors as x_ThirdCustomerTime
          |lateral view explode(data.CreatedTime) exploded_colors as CreatedTime
          |""".stripMargin

      sqlC.sql(sqlStr).createOrReplaceTempView("bk")


      val joinSql = "select bk.*,pr.p_projectId,pr.projName from bk join project pr on bk.ProjGuid=pr.p_projectId"
      val resultDf: DataFrame = sqlC.sql(joinSql).toDF()





      //      ds.foreachRDD { rdd =>
      //        rdd.foreach(println)
      //        rdd
      //          .map(_.value())
      //          .writeToKafka(producerConfig(brokers), transformFunc(outTopic, _))
      //      }


      resultDf.show()

    }


    ds.foreachRDD(rdd => {
      CommitOffset(ssc, ds, rdd)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  //end run


}
