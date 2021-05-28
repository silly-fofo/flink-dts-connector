package com.alibaba.flink.connectors.dts;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import com.alibaba.flink.connectors.dts.formats.internal.record.DtsRecord;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import com.alibaba.flink.connectors.dts.fetcher.DtsKafkaUtil;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** dts subscribe instance can act as flink source. */
public class FlinkDtsConsumer<T> extends RichSourceFunction<T>
        implements CheckpointListener, ResultTypeQueryable<T>, CheckpointedFunction {

    protected static final Logger LOG = LoggerFactory.getLogger(FlinkDtsConsumer.class);

    private FlinkKafkaConsumer flinkKafkaConsumer;

    public FlinkDtsConsumer(
            String brokerUrl,
            String topic,
            String sid,
            String user,
            String password,
            long startupOffsetsTimestamp,
            KafkaDeserializationSchema valueDeserializer) {
        this(brokerUrl, topic, sid, user, password, startupOffsetsTimestamp, valueDeserializer, null);
    }

    public FlinkDtsConsumer(
            String brokerUrl,
            String topic,
            String sid,
            String user,
            String password,
            long startupOffsetsTimestamp,
            KafkaDeserializationSchema valueDeserializer,
            Properties kafkaExtraProps) {

        this.flinkKafkaConsumer =
                new FlinkKafkaConsumer(
                        topic,
                        valueDeserializer,
                        DtsKafkaUtil.getKafkaProperties(
                                brokerUrl, topic, sid, user, password, kafkaExtraProps));

        if (startupOffsetsTimestamp > 0) {
            this.flinkKafkaConsumer.setStartFromTimestamp(startupOffsetsTimestamp);
        } else {
            this.flinkKafkaConsumer.setStartFromGroupOffsets();
        }
    }

    public FlinkDtsConsumer(
            String brokerUrl,
            String topic,
            String sid,
            String user,
            String password,
            long startupOffsetsTimestamp,
            DeserializationSchema valueDeserializer,
            Properties kafkaExtraProps) {

        this.flinkKafkaConsumer =
                new FlinkKafkaConsumer(
                        topic,
                        valueDeserializer,
                        DtsKafkaUtil.getKafkaProperties(
                                brokerUrl, topic, sid, user, password, kafkaExtraProps));

        if (startupOffsetsTimestamp > 0) {
            this.flinkKafkaConsumer.setStartFromTimestamp(startupOffsetsTimestamp);
        } else {
            this.flinkKafkaConsumer.setStartFromGroupOffsets();
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return flinkKafkaConsumer.getProducedType();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        this.flinkKafkaConsumer.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.flinkKafkaConsumer.snapshotState(context);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.flinkKafkaConsumer.initializeState(context);
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        this.flinkKafkaConsumer.open(configuration);
    }

    public FlinkDtsConsumer setStartFromGroupOffsets() {
        this.flinkKafkaConsumer.setStartFromGroupOffsets();
        return this;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        this.flinkKafkaConsumer.run(ctx);
    }

    @Override
    public void cancel() {
        this.flinkKafkaConsumer.cancel();
    }

    public FlinkDtsConsumer assignTimestampsAndWatermarks(
            AssignerWithPeriodicWatermarks<DtsRecord> assigner) {
        this.flinkKafkaConsumer.assignTimestampsAndWatermarks(assigner);
        return this;
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        this.flinkKafkaConsumer.setRuntimeContext(t);
        super.setRuntimeContext(t);
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }
}
