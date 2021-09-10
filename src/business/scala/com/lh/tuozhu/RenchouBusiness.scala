package com.lh.tuozhu

import com.lh.utils.{RdbmsUtils, Schemas, SparkUtils, kfkProperties}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.core.StreamingKafkaContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.dstream.InputDStream

object RenchouBusiness {

  SparkUtils.setHdfsUser("root")
  PropertyConfigurator.configure("conf/log4j.properties")

  def main(args: Array[String]): Unit = {
    run
  }


  /**
   * @fun 具体业务逻辑实现：
   *      认筹，获取s_booking表中insert，且为激活状态到数据
   * @author jwp
   * @date 2021/09/05
   */

  def run() {

    try {

      val ssc: StreamingKafkaContext = SparkUtils.getKfkSccInstall("local[4]", "renchou",
        kfkProperties.BROKER_LIST, kfkProperties.GROUP_ID, "renchou", "LAST",
        "consum", 10)


      //项目维表数据
      val dimPro: DataFrame = RdbmsUtils.getDataFromTable(ssc.sc, "p_project", "p_projectId", "projName", "BUGUID")
      val broadcast = ssc.sc.broadcast(dimPro)

      //机会用户维表数据
      val dimSbook2Cst: DataFrame = RdbmsUtils.getDataFromTable(ssc.sc, "S_BOOKING2CST", "BookingGUID", "OppCstGUID")
      val dimSbook2CstBst = ssc.sc.broadcast(dimSbook2Cst)


      //输出最后一次消费的offset
      //      SparkUtils.getConsumerOffset(kp.toMap).foreach(item => println("上次消费的topic：%s，offset：%s".format(item._1, item._2)))


      val ds: InputDStream[ConsumerRecord[String, String]] = ssc.createDirectStream[String, String](Set(kfkProperties.TOPIC))


      ds.mapPartitions(v => v.map(vv => vv.value())).foreachRDD {
        rdd =>
          broadcast.value.persist().createOrReplaceTempView("project")
          dimSbook2CstBst.value.persist().createOrReplaceTempView("sb2cst")

          val sqlC = SparkUtils.getSQLContextInstance(rdd.sparkContext)
          val df = sqlC.read.schema(Schemas.renchouSchema).json(rdd)
          df.createOrReplaceTempView("booking")
          val sqlStr =
            """
              |select
              |database,
              |table,
              |type,
              |es,
              |BookingGUID,
              |ProjGuid,
              |ProjNum,
              |ProjName,
              |nvl(x_IsTPFCustomer,'') x_IsTPFCustomer,
              |nvl(x_TPFCustomerTime,'') x_TPFCustomerTime,
              |nvl(x_IsThirdCustomer,'') x_IsThirdCustomer,
              |nvl(x_ThirdCustomerTime,'') as x_ThirdCustomerTime,
              |CreatedTime,
              |Status
              |from booking
              |lateral view explode(data.BookingGUID) exploded_names as BookingGUID
              |lateral view explode(data.ProjName) exploded_names as ProjName
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


          val businesSql =
            """
              |select
              |type,
              |es,
              |nvl(pr.BUGUID,'') commpanyId,
              |nvl(bk.BookingGUID,'')bookingGuid,
              |nvl(bk.ProjGuid,'')projectGuid,
              |nvl(bk.ProjNum,'')ProjNum,
              |nvl(bk.ProjName,'') projectName,
              |bk.x_IsTPFCustomer isLevel25,
              |bk.x_TPFCustomerTime level25Time,
              |bk.x_IsThirdCustomer isLevel30,
              |bk.x_ThirdCustomerTime level30Time,
              |bk.CreatedTime createdTime,
              |nvl(bk.Status,'') status,
              |nvl(pr.p_projectId,'') stagingId,
              |nvl(pr.projName,'') projName,
              |nvl(sb.OppCstGUID,'') customerId
              |from bk
              |left join project pr on bk.ProjGuid=pr.p_projectId
              |left join sb2cst sb  on sb.BookingGUID=bk.BookingGUID
              |where type='INSERT' and table='s_booking' -- and bk.Status='激活'
              |""".stripMargin

          val businessDf: DataFrame = sqlC.sql(businesSql)


          businessDf.groupBy("es", "type", "commpanyId", "bookingGuid", "projectGuid", "ProjNum"
            , "projectName", "isLevel25", "level25Time", "isLevel30", "level30Time", "createdTime", "status", "stagingId", "projName")
            .agg(collect_set("customerId").as("customerId"))
            .createOrReplaceTempView("business")


          val joinSql =
            """
              |select
              |'%s' as subject,
              |'%s' as msg ,
              |t.es as eventTime,
              |t.type as actionType,
              |'' as umsId,
              |es as umsTime,
              |type  as umsActive,
              |struct(t.commpanyId,t.bookingGuid,t.projectGuid,t.ProjNum,t.projectName,t.isLevel25,t.level25Time,
              |t.isLevel30,t.level30Time,t.createdTime,t.status,t.stagingId,t.projName,t.customerId) as data
              |from business
              |t""".format("booking", "认筹").stripMargin

          val resultDf: DataFrame = sqlC.sql(joinSql)
          //注意这里不用to_json  嵌套使用，否则json格式会有反斜线

          resultDf
            .selectExpr("cast(data.bookingGuid as String) AS key", "to_json(struct(*)) AS value")
            .write
            .mode("append") //append 追加  overwrite覆盖   ignore忽略  error报错
            .format("kafka")
            .option("ignoreNullFields", "false")
            .option("kafka.bootstrap.servers", kfkProperties.BROKER_LIST)
            .option("topic", kfkProperties.SINK_TOPIC)
            .save()

          resultDf.rdd.checkpoint() //设置检查点，方便失败后数据恢复

      }


      //提交offset
      ds.foreachRDD(rdd => {
        SparkUtils.CommitOffset(ssc, ds, rdd)
      })


      ssc.start()
      ssc.awaitTermination()
    } catch {
      case ex: Exception => {
        println("IO Exception%s".format(ex.getMessage))
      }
    }
  }

  //end run


}
