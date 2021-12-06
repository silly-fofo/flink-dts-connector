package com.alibaba.flink.connectors.dts.formats.internal.record;

public class EtlRow {
    private RowImage rowImage;
    private String tableName;
    private String operationType;
    private long timestamp;

    public EtlRow(RowImage rowImage, String tableName, String operationType, long timestamp) {
        this.rowImage = rowImage;
        this.tableName = tableName;
        this.operationType = operationType;
        this.timestamp = timestamp;
    }

    public RowImage getRowImage() {
        return rowImage;
    }

    public void setRowImage(RowImage rowImage) {
        this.rowImage = rowImage;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
