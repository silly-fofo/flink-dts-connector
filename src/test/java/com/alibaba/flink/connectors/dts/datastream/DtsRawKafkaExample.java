package com.alibaba.flink.connectors.dts.datastream;

import com.alibaba.flink.connectors.dts.FlinkDtsKafkaConsumer;
import com.alibaba.flink.connectors.dts.FlinkDtsRawConsumer;
import com.alibaba.flink.connectors.dts.formats.internal.record.DtsRecord;
import com.alibaba.flink.connectors.dts.formats.internal.record.OperationType;
import com.alibaba.flink.connectors.dts.formats.raw.DtsRecordDeserializationSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;

public class DtsRawKafkaExample {
    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = DtsRawKafkaExampleUtil.prepareExecutionEnv(parameterTool);

        DataStream input = env.addSource(
                new FlinkDtsRawConsumer(
                        parameterTool.get("broker-url"),
                        parameterTool.get("topic"),
                        parameterTool.get("group"),
                        Integer.valueOf(parameterTool.get("checkpoint")),
                        new KafkaDeserializationSchemaWrapper(new DtsRecordDeserializationSchema()))
                        .assignTimestampsAndWatermarks(new DtsCustomWatermarkExtractor()))
                .filter(
                        new FilterFunction<DtsRecord>() {
                            @Override
                            public boolean filter(DtsRecord record) throws Exception {
                                if (OperationType.INSERT == record.getOperationType()
                                        || OperationType.UPDATE == record.getOperationType()
                                        || OperationType.DELETE == record.getOperationType()
                                        || OperationType.HEARTBEAT
                                        == record.getOperationType()) {
                                    return true;
                                } else {
                                    return false;
                                }
                            }
                        })
                .rebalance();

        input.addSink(new PrintSinkFunction<>()).setParallelism(1);

        env.setParallelism(1);

        env.execute("Dts subscribe Example");
    }
}
