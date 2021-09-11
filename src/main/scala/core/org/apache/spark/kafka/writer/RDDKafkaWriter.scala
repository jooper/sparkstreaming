package org.apache.spark.kafka.writer

import org.apache.spark.rdd.RDD
import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
private[spark] class rddKafkaWriter[T](@transient private val rdd: RDD[T]) {
  val log = LoggerFactory.getLogger("StreamingDynamicContext")
  /**
    * @author LMQ
    * @description 将rdd的数据写入kafka
    */
  def writeToKafka[K, V](producerConfig: Properties,
                         transformFunc: T => ProducerRecord[K, V]) {
    rdd.foreachPartition { partition =>
      val producer = KafkaProducerCache.getProducer[K, V](producerConfig)
      partition
        .map(transformFunc)
        .foreach(record => {
          println("发送的数据信息到：%s：%s".format(record.topic(),record))
//          log.info("发送的数据信息："+record)
          producer.send(record)
        })

    }
  }
}
