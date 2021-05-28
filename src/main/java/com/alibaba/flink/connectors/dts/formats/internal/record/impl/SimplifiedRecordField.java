package com.alibaba.flink.connectors.dts.formats.internal.record.impl;

import com.alibaba.flink.connectors.dts.formats.internal.record.RawDataType;
import com.alibaba.flink.connectors.dts.formats.internal.record.RecordField;
import com.alibaba.flink.connectors.dts.formats.internal.record.value.Value;

import java.util.Collections;
import java.util.Set;

/** record id replsent as database column. */
public class SimplifiedRecordField implements RecordField {

    private final String fieldName;
    private final RawDataType rawDataType;
    private boolean isPrimaryKey;
    private boolean isUniqueKey;

    private int fieldPosition;

    public SimplifiedRecordField(String fieldName, RawDataType rawDataType) {
        this.fieldName = fieldName;
        this.rawDataType = rawDataType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Set<String> getAliases() {
        return Collections.emptySet();
    }

    public RawDataType getRawDataType() {
        return rawDataType;
    }

    public RawDataType getSourceRawDataType() {
        return rawDataType;
    }

    public void setSourceRawDataType(RawDataType rawDataType) {
        throw new RuntimeException("does not support this function");
    }

    public Value getDefaultValue() {
        return null;
    }

    public boolean isNullable() {
        return true;
    }

    public boolean isUnique() {
        return isUniqueKey;
    }

    public RecordField setUnique(boolean isUnique) {
        isUniqueKey = isUnique;
        return this;
    }

    public boolean isPrimary() {
        return isPrimaryKey;
    }

    public boolean setPrimary(boolean isPrimary) {
        isPrimaryKey = isPrimary;
        return isPrimaryKey;
    }

    public boolean isIndexed() {
        return isPrimaryKey || isUniqueKey;
    }

    public boolean isAutoIncrement() {
        return false;
    }

    public int keySeq() {
        return 0;
    }

    public int getFieldPosition() {
        return fieldPosition;
    }

    public void setFieldPosition(int fieldPosition) {
        this.fieldPosition = fieldPosition;
    }

    @Override
    public int getDisplaySize() {
        return 0;
    }

    public int getScale() {
        return 0;
    }
}
