package com.alibaba.flink.connectors.dts.formats.internal.record.value;

/** none value. */
public class NoneValue implements Value<Boolean> {

    @Override
    public ValueType getType() {
        return ValueType.NONE;
    }

    @Override
    public Boolean getData() {
        return false;
    }

    @Override
    public long size() {
        return 0L;
    }
}
