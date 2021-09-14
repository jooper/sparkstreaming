package org.apache.spark.core

trait SparkKafkaConfsKey {
  val GROUPID = "group.id"
  val BROKER = "metadata.broker.list"
  val BOOTSTRAP = "bootstrap.servers"
  val SERIALIZER = "serializer.class"
  val VALUE_DESERIALIZER = "value.deserializer"
  val KEY_DESERIALIZER = "key.deserializer"
  val AUTO_COMMIT = "enable.auto.commit"
  val KEY_DESERIALIZER_ENCODE = "key.deserializer.encoding"
  val VALUE_DESERIALIZER_ENCODE = "value.deserializer.encoding"
  val AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset"
  val ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit"
  val RECEIVE_BUFFER_CONFIG = "receive.buffer.bytes"
  /*
   * 如果groupid不存在或者过期选择从last还是从earliest开始
   */
  val WRONG_GROUP_FROM = "wrong.groupid.from"
  /*
   * 从last还是从consumer开始
   */
  val CONSUMER_FROM = "kafka.consumer.from"
  val DEFUALT_FROM = CONSUM
  val LAST = "LAST"
  val CONSUM = "CONSUM"
  val EARLIEST = "EARLIEST"
  val CUSTOM = "CUSTOM"
  val KAFKA_OFFSET = "kafka.offset"
  val KAFKAOFFSET = "kafka.offset"
  val MAX_RATE_PER_PARTITION = "spark.streaming.kafka.maxRatePerPartition"

  val DRIVER_SSL_TRUSTSTORE_LOCATION = "driver.ssl.truststore.location" //设置driver端的ssl文件路径
  val DRIVER_SSL_KEYSTORE_LOCATION = "driver.ssl.keystore.location"

  val EXECUTOR_SSL_TRUSTSTORE_LOCATION = "executor.ssl.truststore.location" //设置 executor端的ssl文件路径
  val EXECUTOR_SSL_KEYSTORE_LOCATION = "executor.ssl.keystore.location"

  val SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location"
  val SSL_KEYSTORE_LOCATION = "ssl.keystore.location"
  val MAX_PARTION_FETCH_BYTES = "max.partition.fetch.bytes"

  /**
   * @author LMQ
   * @description 获取kafka的配置，一般不做特殊的配置，用这个就够了
   * @param brokers       :kafka brokers
   * @param groupid       :kafka groupid
   * @param consumer_from :kafak 从哪开始消费
   *                      last ： 从最新数据开始
   *                      earliest ：从最早数据开始
   *                      consum： 从上次消费点继续
   *                      custom：自定义消费
   * @param wrong_from    ：如果kafka的offset出现问题，导致你读不到，这里配置是从哪里开始读取
   * @param kafkaoffset   ： 自定义offset
   * @desc 这里返回的是可变的map，在外部可以自行添加ssl
   */
  def getKafkaParam(brokers: String,
                    groupid: String,
                    consumer_from: String,
                    wrong_from: String,
                    kafkaoffset: String = "") = {
    scala.collection.mutable.Map[String, String](
      BROKER -> brokers,
      BOOTSTRAP -> brokers,
      AUTO_COMMIT -> "false", //默认不自动提交
      SERIALIZER -> "kafka.serializer.StringEncoder",
      KEY_DESERIALIZER -> "org.apache.kafka.common.serialization.StringDeserializer",
      VALUE_DESERIALIZER -> "org.apache.kafka.common.serialization.StringDeserializer",
      VALUE_DESERIALIZER_ENCODE -> "UTF8",
      ENABLE_AUTO_COMMIT_CONFIG -> "false",
      KEY_DESERIALIZER_ENCODE -> "UTF8",
      GROUPID -> groupid,
      WRONG_GROUP_FROM -> wrong_from, //EARLIEST
      CONSUMER_FROM -> consumer_from, //如果是配置了CUSTOM。必须要配一个 kafka.offset的参数
      KAFKAOFFSET -> kafkaoffset,
      "max.partition.fetch.bytes" -> "629145600", //指定了服务器从每个分区里返回给消费者的最大字节数
      "max.request.size" -> "1000", //于控制生产者发送的请求大小，可以指能发送的单个消息的最大值，也可以指单个请求里面所有消息总的大小
      "max.block.ms" -> "300000", //配置控制了KafkaProducer.send（）和KafkaProducer.partitionsFor（）的阻塞时间
      "request.timeout.ms" -> "3000000", //指定了生产者在发送数据时等待服务器返回响应的时间
      "max.poll.records" -> "10000", //用于控制单次调用call () 方法能够返回的记录数量，可以帮你控制在轮询里需要处理的数据量
      "fetch.message.max.bytes" -> "100000000",
      "fetch.min.bytes" -> "1000000", //定了消费者从服务器消费数据的最小字节数
      "message.max.bytes" -> "102400",
      "fetch.max.wait.ms" -> "20", //用于指定 broker 的等待时间
      "reconnect.backoff.ms" -> "0" //如果客户端与kafka的broker的连接断开了，客户端会等reconnect.backoff.ms之后重新连接
    )
  }
}
