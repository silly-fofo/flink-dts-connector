package com.alibaba.flink.connectors.dts.formats.internal.record.value;

/** ValueType. */
public enum ValueType {
    BIT,
    INTEGER_NUMERIC,
    FLOAT_NUMERIC,
    DECIMAL_NUMERIC,
    SPECIAL_NUMERIC,
    STRING,
    DATETIME,
    UNIX_TIMESTAMP,
    TEXT_ENCODING_OBJECT,
    BINARY_ENCODING_OBJECT,
    WKB_GEOMETRY,
    WKT_GEOMETRY,
    NONE
}
