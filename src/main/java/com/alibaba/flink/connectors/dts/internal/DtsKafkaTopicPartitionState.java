package com.alibaba.flink.connectors.dts.internal;

import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;

public class DtsKafkaTopicPartitionState<T, KPH> extends KafkaTopicPartitionState<T, KPH> {

    private volatile long timestamp;

    public DtsKafkaTopicPartitionState(KafkaTopicPartition partition, KPH kafkaPartitionHandle) {
        super(partition, kafkaPartitionHandle);

        this.timestamp = Long.MIN_VALUE;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
