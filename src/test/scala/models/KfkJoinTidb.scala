package models

import java.util.Arrays

import com.lh.utils.{RdbmsUtils, SparkUtils}
import com.spark.test.KafkaProperties
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.common.util.KafkaConfig
import org.apache.spark.core.StreamingKafkaContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

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
    val dimPro: DataFrame = RdbmsUtils.mysql.getDataFromTable(sc, "p_project", "p_projectId", "projName")
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

      df.printSchema()
      df.createOrReplaceTempView("booking")


      //      df.select(expr("explode(data.BgnDate)"), col("BookingGUID")).show()
      //      df.selectExpr(explode("data.BgnDate"), col("BookingGUID")).show()


      var sqlStr =
        """
          |SELECT
          |bk.data.BookingGUID,
          |bk.data.projGuid  companyid,
          |pr.p_projectId projectid
          |FROM booking bk
          |join p_project pr on cast(bk.data.projGuid as String)=pr.p_projectId
          |""".stripMargin

      sqlStr = "select explode(data.BgnDate) as BgnDate  from booking bk"

      sqlC.sql(sqlStr).show()
      //      sqlC.sql("select database,table,type,data.BgnDate projGuid,data.BookingGUID from booking").show()
      //      df.select("database").show()
      import sqlC.implicits._


      //      df.select(df.col("data").alias("x").toString()).show()


      //      val jsonSchema: String = df.select(schema_of_json(col("database"))).as[String].first


      //      df.select(schema_of_json(df.toString).alias("schema"))
      //        .select(regexp_extract($"schema","struct<(.*?)>",1) as "schema").show(false)


      //      df.select($"database",from_json(col("data"), jsonSchema, Map[String, String]())).toDF().show()

      //      df.select(to_json(struct($"data"))).toDF("devices").show()
      //      df.selectExpr("CAST(data AS STRING)").show()
      //            df.select(explode($"data")).show()
      //            PeopleDf.toDF().select($"database",$"table",$"type",get_json_object($"data","$.BgnDate").alias("BgnDate")).show()

      //      PeopleDf.filter(PeopleDf.col("data")).show()
      //      PeopleDf.select("data.BookingGUID").show()


      //      val dfDetails = PeopleDf.select(PeopleDf("database"),explode(PeopleDf("data.BookingGUID"))).toDF("database","addressInfo").show()

      //      PeopleDf.select(explode($"data")).toDF("data").show()

      //      PeopleDf.toDF().select(from_json($"device", jsonSchema) as"devices")
      //      val PeopleDfFilter = PeopleDf.filter(($"value1".rlike("1")) || ($"value2" === 2))
      //      PeopleDfFilter.show()
    }


    //
    //    var s_bookingsDs = ds.transform(rdd => {
    //
    //      val df = rdd.map(w => {
    //        val jsonObject = JsonUtils.gson(w.value())
    //        val dt = jsonObject.get("data").getAsJsonArray.iterator()
    //
    //
    //        val s_bookings = dt.map(item => {
    //          val dt = item.getAsJsonObject
    //
    //          S_booking(jsonObject.get("database").toString, jsonObject.get("table").toString, jsonObject.get("type").toString,
    //            dt.get("BookingGUID").toString, dt.get("BookingGUID").toString, dt.get("BookingGUID").toString,
    //            dt.get("BookingGUID").toString, dt.get("BookingGUID").toString, dt.get("BookingGUID").toString,
    //            dt.get("BookingGUID").toString, dt.get("BookingGUID").toString, dt.get("BookingGUID").toString
    //            , dt.get("CreatedTime").toString)
    //        })
    //        s_bookings.toSeq
    //      })
    //      df
    //    })
    //
    //
    //
    //
    //
    //    s_bookingsDs.transform(rdd => {
    //      val sqlC = SparkUtils.getSQLContextInstance(rdd.sparkContext)
    //      import sqlC.implicits._
    //      rdd.toDF().createOrReplaceTempView("booking")
    //      sqlC.sql("select value.database,value.table from booking").rdd
    //    }).print()


    //    s_bookingsDs.print()

    //
    //      val df1 = df.cache()
    //
    //
    //      df1.select(
    //        expr("explode(data.BgnDate)") as "BgnDate",
    //        col("database"),
    //        col("table"),
    //        col("type"),
    //        col("data")
    //      ).select(
    //        col("database"),
    //        col("table"),
    //        col("type"),
    //        col("data"),
    //        col("BgnDate"),
    //        expr("explode(data.BookingGUID)") as "BookingGUID")
    //      //        .show()
    //
    //
    //      sqlStr =
    //        """
    //          |SELECT
    //          |explode(bk.data.BookingGUID) BookingGUID,
    //          |explode(bk.data.projGuid)  projGuid,
    //          |pr.p_projectId projectid
    //          |FROM booking bk
    //          |left join p_project pr on explode(bk.data.projGuid)=pr.p_projectId
    //          |""".stripMargin
    //


    val joinSql =
      """
        |select to_json(struct(tt.*)) as value from
        |(select 'booking' as subject,'认筹' as msg ,cast(struct(t.*) as String) as data from (
        |select
        |nvl(pr.BUGUID,'') commpanyId,
        |nvl(bk.BookingGUID,'')BookingGUID,
        |nvl(bk.ProjGuid,'')ProjGuid,
        |nvl(bk.ProjNum,'')ProjNum,
        |bk.x_IsTPFCustomer,
        |bk.x_TPFCustomerTime,
        |bk.x_IsThirdCustomer,
        |bk.x_ThirdCustomerTime,
        |bk.CreatedTime,
        |nvl(bk.Status,'') Status,
        |nvl(pr.p_projectId,'') p_projectId,
        |nvl(pr.projName,'') projName,
        |nvl(sb.OppCstGUID,'') OppCstGUID
        |from bk
        |left join project pr on bk.ProjGuid=pr.p_projectId
        |left join sb2cst sb  on sb.BookingGUID=bk.BookingGUID
        |where type='INSERT' and table='s_booking' -- and bk.Status='激活'
        |)t)tt""".stripMargin


    ssc.start()
    ssc.awaitTermination()
  }

  //end run

}
