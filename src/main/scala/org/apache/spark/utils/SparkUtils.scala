package org.apache.spark.utils

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {
  @transient private var sparkInstance: SparkContext = _
  @transient private var sqlInstance: SQLContext = _

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
      )
    }
    sparkInstance
  }
}
