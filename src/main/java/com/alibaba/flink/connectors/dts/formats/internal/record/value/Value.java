package com.alibaba.flink.connectors.dts.formats.internal.record.value;

/** Value. */
public interface Value<T> {
    /*
     * 获取Value定义类型
     */
    ValueType getType();

    /*
     * Get the internal data of current value.
     */
    T getData();

    /*
     * Convert current to string by utf-8 encoding.
     */
    String toString();

    /*
     * Get the size of current value.
     */
    long size();
}
