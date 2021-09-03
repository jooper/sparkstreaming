package com.spark.test

/**
 * 配置属性常量
 */
object KafkaProperties {
  val ZK = "10.231.149.36:2181,10.231.149.37:2181" //Zookeeper地址

  val TOPIC = "ods_crm2" //topic名称

  val SINK_TOPIC = "ods_crm_sink"
  val BROKER_LIST = "10.231.149.36:9092,10.231.149.37:9092" //Broker列表

  val GROUP_ID = "ods_crm2" //消费者使用

  val AUTO_OFFSET_RESET_CONFIG = "smallest" //smallest,largest,earliest,latest

}
