package com.lh.utils

object ConfigUtils {
  //  val ZK = "10.231.149.36:2181,10.231.149.37:2181" //Zookeeper地址
  //  val TOPIC = "ods_crm2" //topic名称
  //  val SINK_TOPIC = "ods_crm_sink"
  //  val BROKER_LIST = "10.231.149.36:9092,10.231.149.37:9092" //Broker列表
  //  val GROUP_ID = "ods_crm2" //消费者使用
  //  val AUTO_OFFSET_RESET_CONFIG = "LAST" //smallest,largest,earliest,   LAST
  //  val HDFS = "hdfs://10.231.145.212:9000"

  //可从配置文档中读取
  val cnf = PropertiesUtils.Init
  val ZK = cnf.getProperty("zk") //Zookeeper地址
  val TOPIC = cnf.getProperty("source.topic") //topic名称
  val SINK_TOPIC = cnf.getProperty("sink.topic")
  val BROKER_LIST = cnf.getProperty("kafka.broker") //Broker列表
  val GROUP_ID = cnf.getProperty("kafka.group.id") //消费者使用
  val CONSUMER_FROM = cnf.getProperty("kafka.consumer.from") //smallest,largest,earliest,   LAST
  val WRONG_FROM = cnf.getProperty("wrong.groupid.from") //smallest,largest,earliest,   LAST
  val HDFS = cnf.getProperty("hdfs.path")

}
