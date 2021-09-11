package org.apache.spark.kafka.partitioners;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;


/**
 * @author jwp
 * @fun 自定义判客到访分区器，将不同业务数据放不不同的kafka分区中
 * @date 2021/09/09
 **/
public class TuozPartitioner implements Partitioner {
    //    @Override
//    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
//        return 8;
//    }

    /**
     * 自定义kafka分区主要解决用户分区数据倾斜问题 提高并发效率（假设 3 分区）
     *
     * @param topic      消息队列名
     * @param key        用户传入key
     * @param keyBytes   key字节数组
     * @param value      用户传入value
     * @param valueBytes value字节数据
     * @param cluster    当前kafka节点数
     * @return 如果3个节点数 返回 0 1 2 如果5个 返回 0 1 2 3 4 5
     */

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {


        // 得到 topic 的 partitions 信息
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        // 模拟某客服
//        if (key.toString().equals("10000") || key.toString().equals("11111")) {
//            // 放到最后一个分区中
//            return numPartitions - 1;
//        }
//        String phoneNum = key.toString();
//        return phoneNum.substring(0, 3).hashCode() % (numPartitions - 1);
        return 0;
    }


    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
