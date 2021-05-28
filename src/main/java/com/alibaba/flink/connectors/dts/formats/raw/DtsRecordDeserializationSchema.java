package com.alibaba.flink.connectors.dts.formats.raw;


import com.alibaba.flink.connectors.dts.formats.internal.record.DtsRecord;
import com.alibaba.flink.connectors.dts.formats.internal.record.impl.LazyParseRecordImpl;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

/** DtsRowDeserializationSchema. */
public class DtsRecordDeserializationSchema extends AbstractDeserializationSchema<DtsRecord> {
    @Override
    public DtsRecord deserialize(byte[] message) throws IOException {
        return new LazyParseRecordImpl(message);
    }
}
