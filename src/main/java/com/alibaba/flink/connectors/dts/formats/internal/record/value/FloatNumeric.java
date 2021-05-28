package com.alibaba.flink.connectors.dts.formats.internal.record.value;

/** FloatNumeric. */
public class FloatNumeric implements Value<Double> {

    private Double data;

    public FloatNumeric(Double data) {
        this.data = data;
    }

    @Override
    public ValueType getType() {
        return ValueType.FLOAT_NUMERIC;
    }

    @Override
    public Double getData() {
        return this.data;
    }

    @Override
    public String toString() {
        return Double.toString(this.data);
    }

    @Override
    public long size() {
        return Double.BYTES;
    }
}
