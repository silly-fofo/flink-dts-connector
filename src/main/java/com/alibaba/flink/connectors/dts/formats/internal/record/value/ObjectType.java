package com.alibaba.flink.connectors.dts.formats.internal.record.value;

/** ObjectType. */
public enum ObjectType {
    BINARY,
    BOOL,
    BLOB,
    XML,
    JSON,
    TEXT,
    BFILE,
    RAW,
    LONG_RAW,
    ROWID,
    UROWID,
    ENUM,
    SET,
    BYTEA,
    GEOMETRY,
    XTYPE;

    public static ObjectType parse(String type) {

        if (null == type) {
            return XTYPE;
        }
        type = type.toUpperCase();

        ObjectType[] objectTypes = ObjectType.values();
        for (ObjectType objectType : objectTypes) {
            if (objectType.name().equals(type)) {
                return objectType;
            }
        }
        return XTYPE;
    }
}
