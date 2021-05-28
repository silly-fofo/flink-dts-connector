package com.alibaba.flink.connectors.dts.formats.internal.record.impl;

import com.alibaba.flink.connectors.dts.formats.internal.common.NullableOptional;
import com.alibaba.flink.connectors.dts.formats.internal.record.*;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** schema. */
public class LazyRecordSchema implements RecordSchema {

    private LazyParseRecordImpl record;

    private DatabaseInfo databaseInfo;
    private final String schemaId;
    private final String databaseName;
    private final String tableName;
    private String fullQualifiedName;

    private String logicalDatabaseName;
    private String logicalTableName;

    private List<RecordField> recordFields;
    private Map<String, Integer> recordFieldIndex;
    protected RecordIndexInfo primaryIndexInfo;
    protected List<RecordIndexInfo> uniqueIndexInfo;
    private String condition;

    public LazyRecordSchema(
            LazyParseRecordImpl record, String schemaId, String databaseName, String tableName) {

        this.record = record;
        this.schemaId = schemaId;
        this.databaseName = databaseName;
        this.tableName = tableName;

        if (!StringUtils.isEmpty(databaseName) && !StringUtils.isEmpty(tableName)) {
            this.fullQualifiedName = databaseName + "." + tableName;
        }

        this.uniqueIndexInfo = new ArrayList<>(1);
    }

    public void setDatabaseInfo(DatabaseInfo databaseInfo) {
        this.databaseInfo = databaseInfo;
    }

    protected void initPayloadIfNeeded() {
        synchronized (this) {
            this.record.initPayloadIfNeeded();
        }
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return this.databaseInfo;
    }

    public void setRecordFields(List<RecordField> recordFields) {
        if (null != recordFields && !recordFields.isEmpty()) {

            this.recordFields = new ArrayList<>(recordFields.size());
            this.recordFieldIndex = new HashMap<>();

            for (RecordField field : recordFields) {
                this.recordFields.add(field);
                this.recordFieldIndex.put(field.getFieldName(), field.getFieldPosition());
            }
        }
    }

    @Override
    public List<RecordField> getFields() {
        initPayloadIfNeeded();
        return this.recordFields;
    }

    @Override
    public int getFieldCount() {
        return getFieldCount(true);
    }

    public int getFieldCount(boolean load) {
        if (load) {
            initPayloadIfNeeded();
        }
        return null == this.recordFields ? -1 : this.recordFields.size();
    }

    @Override
    public RecordField getField(int index) {
        initPayloadIfNeeded();
        return this.recordFields.get(index);
    }

    @Override
    public NullableOptional<RecordField> getField(String fieldName) {
        return this.getField(fieldName, true);
    }

    public NullableOptional<RecordField> getField(String fieldName, boolean load) {
        if (load) {
            initPayloadIfNeeded();
        }
        Integer index = this.recordFieldIndex.get(fieldName);
        if (null == index) {
            return NullableOptional.empty();
        }
        return NullableOptional.of(this.recordFields.get(index));
    }

    @Override
    public List<RawDataType> getRawDataTypes() {
        initPayloadIfNeeded();
        return recordFields.stream().map(RecordField::getRawDataType).collect(Collectors.toList());
    }

    @Override
    public List<String> getFieldNames() {
        initPayloadIfNeeded();
        return recordFields.stream().map(RecordField::getFieldName).collect(Collectors.toList());
    }

    @Override
    public NullableOptional<RawDataType> getRawDataType(String fieldName) {
        initPayloadIfNeeded();
        RecordField recordField = getField(fieldName).get();
        if (null == recordField) {
            return NullableOptional.empty();
        } else {
            return NullableOptional.of(recordField.getRawDataType());
        }
    }

    @Override
    public NullableOptional<String> getFullQualifiedName() {
        return StringUtils.isEmpty(fullQualifiedName)
                ? NullableOptional.empty()
                : NullableOptional.of(fullQualifiedName);
    }

    @Override
    public NullableOptional<String> getDatabaseName() {
        if (null == logicalDatabaseName) {
            return StringUtils.isEmpty(this.databaseName)
                    ? NullableOptional.empty()
                    : NullableOptional.of(this.databaseName);
        }
        return NullableOptional.of(logicalDatabaseName);
    }

    @Override
    public NullableOptional<String> getSchemaName() {
        return getDatabaseName();
    }

    @Override
    public NullableOptional<String> getTableName() {
        if (null == logicalTableName) {
            return StringUtils.isEmpty(tableName)
                    ? NullableOptional.empty()
                    : NullableOptional.of(tableName);
        }
        return NullableOptional.of(logicalTableName);
    }

    @Override
    public String getSchemaIdentifier() {
        return schemaId;
    }

    public void setPrimaryIndexInfo(RecordIndexInfo indexInfo) {
        this.primaryIndexInfo = indexInfo;
    }

    @Override
    public RecordIndexInfo getPrimaryIndexInfo() {
        initPayloadIfNeeded();
        return this.primaryIndexInfo;
    }

    @Override
    public List<ForeignKeyIndexInfo> getForeignIndexInfo() {
        return Collections.emptyList();
    }

    public void addUniqueIndexInfo(RecordIndexInfo indexInfo) {
        this.uniqueIndexInfo.add(indexInfo);
    }

    @Override
    public List<RecordIndexInfo> getUniqueIndexInfo() {
        initPayloadIfNeeded();
        return this.uniqueIndexInfo;
    }

    @Override
    public List<RecordIndexInfo> getNormalIndexInfo() {
        return Collections.emptyList();
    }

    @Override
    public String getFilterCondition() {
        return this.condition;
    }

    @Override
    public void initFilterCondition(String condition) {
        this.condition = condition;
    }

    void setLogicalDatabaseName(String name) {
        this.logicalDatabaseName = name;
    }

    void setLogicalTableName(String name) {
        this.logicalTableName = name;
    }
}
