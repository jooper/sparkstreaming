package models

import java.io.{FileNotFoundException, IOException}
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
import org.apache.spark.utils.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._


object KfkJoinTidb {
  val brokers = KafkaProperties.BROKER_LIST
  System.setProperty("HADOOP_USER_NAME", "root")
  PropertyConfigurator.configure("conf/log4j.properties")

  def main(args: Array[String]): Unit = {
    try {
      run
    } catch {
      case ex: IOException => {
        println("IO Exception%s".format(ex.getMessage))
      }
    }
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

    println(rdd.partitions.foreach("partition:%s".format(_)))
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
    sc.setCheckpointDir("hdfs://10.231.145.212:9000/sparkCheckPoint")
    sc.getConf.set("per.partition.offsetrange.step", "1000")
    sc.getConf.set("per.partition.offsetrange.threshold", "1000")
    sc.getConf.set("enable.auto.repartion", "false")


    val kp = StreamingKafkaContext.getKafkaParam(brokers, KafkaProperties.GROUP_ID,
      KafkaProperties.AUTO_OFFSET_RESET_CONFIG, KafkaProperties.AUTO_OFFSET_RESET_CONFIG)
    val ssc = new StreamingKafkaContext(kp.toMap, sc, Seconds(10))


    //项目维表数据
    val dimPro: DataFrame = RdbmsUtils.getDataFromTable(sc, "p_project", "p_projectId", "projName", "BUGUID")
      .persist(StorageLevel.MEMORY_ONLY)
    val broadcast = sc.broadcast(dimPro)

    val dimSbook2Cst: DataFrame = RdbmsUtils.getDataFromTable(sc, "S_BOOKING2CST", "BookingGUID", "OppCstGUID")
      .persist(StorageLevel.MEMORY_ONLY)
    val dimSbook2CstBst = sc.broadcast(dimSbook2Cst)

    //    dimSbook2Cst.show(10)

    //输出最后一次消费的offset
    getConsumerOffset(kp.toMap).foreach(item => println("上次消费的topic：%s，offset：%s".format(item._1, item._2)))
    val ds: InputDStream[ConsumerRecord[String, String]] = ssc.createDirectStream[String, String](Set(KafkaProperties.TOPIC))
    broadcast.value.persist().createOrReplaceTempView("project")
    dimSbook2CstBst.value.persist().createOrReplaceTempView("sb2cst")
    ds.map(v => v.value).foreachRDD {
      rdd =>
        val sqlC = SparkUtils.getSQLContextInstance(rdd.sparkContext)
        val df = sqlC.read.schema(Schemas.sbookingSchema3).json(rdd)
        import sqlC.implicits._
        df.createOrReplaceTempView("booking")
        val sqlStr =
          """
            |select
            |database,
            |table,
            |type,
            |BookingGUID,
            |ProjGuid,
            |ProjNum,
            |nvl(x_IsTPFCustomer,'') x_IsTPFCustomer,
            |nvl(x_TPFCustomerTime,'') x_TPFCustomerTime,
            |nvl(x_IsThirdCustomer,'') x_IsThirdCustomer,
            |nvl(x_ThirdCustomerTime,'') as x_ThirdCustomerTime,
            |CreatedTime,
            |Status
            |from booking
            |lateral view explode(data.BookingGUID) exploded_names as BookingGUID
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


        val joinSql =
          """
            |select
            |'booking' as subject,
            |'认筹' as msg ,
            |struct(t.*) as data
            |from (
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
            |)t""".stripMargin
        val resultDf: DataFrame = sqlC.sql(joinSql)
        //注意这里不用to_json  嵌套使用，否则json格式会有反斜线

        resultDf.selectExpr("'booking' AS key", "to_json(struct(*)) AS value")
          .show(1, false)

        resultDf
          .selectExpr("'booking' AS key", "to_json(struct(*)) AS value")
          .write
          .mode("append") //append 追加  overwrite覆盖   ignore忽略  error报错
          .format("kafka")
          .option("ignoreNullFields", "false")
          .option("kafka.bootstrap.servers", KafkaProperties.BROKER_LIST)
          .option("topic", KafkaProperties.SINK_TOPIC)
          .save()


        resultDf.rdd.checkpoint() //设置检查点，方便失败后数据恢复

    }
    //提交offset
    ds.foreachRDD(rdd => {
      CommitOffset(ssc, ds, rdd)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  //end run


}
