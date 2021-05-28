package com.alibaba.flink.connectors.dts.formats.internal.record;

/** represent all data types for all data sources. */
public interface RawDataType {

    String getTypeName();

    int getTypeId();

    boolean isLobType();

    String getEncoding();
}
