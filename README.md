# :tada:spark-util
---------------------
> - Spark 与其他组件结合的代码 <br/>
> - Spark-Kafka <br/>
---------------------
# :tada:branch-1.6.0-0.10
---------------------
> - 解决了批次计算延迟后出现的任务append导致整体恢复后 计算消费还是跟不上的问题
> - 支持动态调节 streaming 的 批次间隔时间 （不同于sparkstreaming 的 定长的批次间隔，StructuredStreaming中使用trigger实现了。） <br/>
> - 支持在streaming过程中 重设 topics，用于生产中动态地增加删减数据源 <br/>
> - 添加了速率控制，KafkaRateController。用来控制读取速率，由于不是用的sparkstreaming，所有速率控制的一些参数拿不到，得自己去计算。
> - 提供spark-streaming-kafka-0-10_2.10 spark 1.6 来支持 kafka的ssl <br/>
> - 支持rdd.updateOffset 来管理偏移量。 <br/>
---------------------
# :tada: branch-sparkstreaming-1.6.0-0.10
---------------------
> - 只是结合了 sparkstreaming 1.6 和 kafka 010 。 使低版本的spark能够使用kafka的ssl验证 <br>
> - 支持 SSL
> - 支持spark 1.6 和 kafka 0.10 的结合
> - 支持管理offset
--------------------
# :tada:branch-2.0.1-0.10
-------------------
> - 解决了批次计算延迟后出现的任务append导致整体恢复后 计算消费还是跟不上的问题
> - 支持动态调节 streaming 的 批次间隔时间 （不同于sparkstreaming 的 定长的批次间隔，StructuredStreaming中使用trigger实现了。） <br/>
> - 支持在streaming过程中 重设 topics，用于生产中动态地增加删减数据源 <br/>
> - 提供spark-streaming-kafka-0-10_2.10 spark 1.6 来支持 kafka的ssl <br/>
> - 支持rdd.updateOffset 来管理偏移量。 <br/>
> - 由于kakfa-010 的api的变化，之前的 kafka-08 版本的 spark-kafka 虽然能用，但是他依赖于spark-streaming-kafka-0-8_2.10 <br/>.(可能会导致一些版本问题)；所以这次重新写了一个 kafka010 & spark-2.x 版本 ；但是使用方法还是跟之前的差不多， <br/>
> - kafka010有两种来管理offset的方式，一种是旧版的用zookeeper来管理，一种是本身自带的。现只提供zookeeper的管理方式
> - 要确保编译的kafka-client的版本和服务器端的版本一致，否则会报 Error reading string of length 27489, only 475 bytes available 等错误<br/>
> - 添加了速率控制，KafkaRateController。用来控制读取速率，由于不是用的sparkstreaming，所有速率控制的一些参数拿不到，得自己去计算。<br>
-------------------
