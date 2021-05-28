package com.alibaba.flink.connectors.dts;

import com.alibaba.flink.connectors.dts.formats.internal.record.DtsRecord;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/** DtsCustomWatermarkExtractor. */
public class DtsCustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<DtsRecord> {

    private static final long serialVersionUID = -742759155861320823L;

    private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(DtsRecord dtsRecord, long previousElementTimestamp) {
        // the inputs are assumed to be of format (message,timestamp)
        this.currentTimestamp = dtsRecord.getTimestamp();
        return dtsRecord.getTimestamp();
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(
                currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
    }
}