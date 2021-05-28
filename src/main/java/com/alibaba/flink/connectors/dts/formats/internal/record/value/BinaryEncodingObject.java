package com.alibaba.flink.connectors.dts.formats.internal.record.value;

import com.alibaba.flink.connectors.dts.formats.internal.common.BytesUtil;

import java.nio.ByteBuffer;

/** BinaryEncodingObject. */
public class BinaryEncodingObject implements Value<ByteBuffer> {

    private ObjectType objectType;
    private ByteBuffer binaryData;

    public BinaryEncodingObject(ObjectType objectType, ByteBuffer binaryData) {
        this.objectType = objectType;
        this.binaryData = binaryData;
    }

    @Override
    public ValueType getType() {
        return ValueType.BINARY_ENCODING_OBJECT;
    }

    @Override
    public ByteBuffer getData() {
        return binaryData;
    }

    public ObjectType getObjectType() {
        return this.objectType;
    }

    @Override
    public long size() {
        if (null != binaryData) {
            return binaryData.capacity();
        }

        return 0L;
    }

    public String toString() {
        return BytesUtil.byteBufferToHexString(binaryData);
    }
}
