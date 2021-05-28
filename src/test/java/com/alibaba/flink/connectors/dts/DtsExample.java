package com.alibaba.flink.connectors.dts;

import com.alibaba.flink.connectors.dts.formats.internal.record.DtsRecord;
import com.alibaba.flink.connectors.dts.formats.internal.record.OperationType;
import com.alibaba.flink.connectors.dts.formats.raw.DtsRecordDeserializationSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;

/** DtsExample. */
public class DtsExample {
    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = DtsExampleUtil.prepareExecutionEnv(parameterTool);

        DataStream input = env.addSource(
                new FlinkDtsKafkaConsumer(
                        parameterTool.get("broker-url"),
                        parameterTool.get("topic"),
                        parameterTool.get("sid"),
                        parameterTool.get("user"),
                        parameterTool.get("password"),
                        Integer.valueOf(parameterTool.get("checkpoint")),
                        new KafkaDeserializationSchemaWrapper(new DtsRecordDeserializationSchema()),
                        null)
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
