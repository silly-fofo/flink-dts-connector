package com.alibaba.flink.connectors.dts;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public abstract class FlinkDtsConsumer <T> extends RichSourceFunction<T>
        implements CheckpointListener, ResultTypeQueryable<T>, CheckpointedFunction {
}
