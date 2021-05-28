package com.alibaba.flink.connectors.dts.formats.internal.record.value;

import com.alibaba.flink.connectors.dts.formats.internal.common.BytesUtil;
import com.vividsolutions.jts.io.ParseException;
import com.alibaba.flink.connectors.dts.formats.internal.common.GeometryUtil;

import java.nio.ByteBuffer;

/** WKBGeometry. */
public class WKBGeometry implements Value<ByteBuffer> {

    private long srid;
    private ByteBuffer data;

    public WKBGeometry(ByteBuffer data) {
        this.data = data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }

    @Override
    public ValueType getType() {
        return ValueType.WKB_GEOMETRY;
    }

    @Override
    public ByteBuffer getData() {
        return this.data;
    }

    @Override
    public long size() {
        if (null != data) {
            return data.capacity();
        }

        return 0L;
    }

    public String toString() {
        try {
            return GeometryUtil.fromWKBToWKTText(data);
        } catch (ParseException ex) {
            return BytesUtil.byteBufferToHexString(data);
        }
    }

    public WKBGeometry parse(Object rawData) {
        if (null == rawData) {
            return null;
        }

        if (rawData instanceof byte[]) {
            return new WKBGeometry(ByteBuffer.wrap((byte[]) rawData));
        }

        return new WKBGeometry(BytesUtil.hexStringToByteBuffer(rawData.toString()));
    }
}
