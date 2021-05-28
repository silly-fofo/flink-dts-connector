package com.alibaba.flink.connectors.dts.formats.internal.record.impl;

/** database source types. */
public enum SourceType {
    MySQL,
    Oracle,
    SQLServer,
    PostgreSQL,
    MongoDB,
    Redis,
    DB2,
    PPAS,
    DRDS,
    HBASE,
    HDFS,
    FILE,
    TIDB,
    OTHER;
    private static final org.apache.avro.Schema SCHEMA$ =
            new org.apache.avro.Schema.Parser()
                    .parse(
                            "{\"type\":\"enum\",\"name\":\"SourceType\",\"namespace\":\"com.alibaba.amp.any.common.record.formats.avro\",\"symbols\":[\"MySQL\",\"Oracle\",\"SQLServer\",\"PostgreSQL\",\"MongoDB\",\"Redis\",\"DB2\",\"PPAS\",\"DRDS\",\"HBASE\",\"HDFS\",\"FILE\",\"TIDB\",\"OTHER\"]}");

    public static org.apache.avro.Schema getClassSchema() {
        return SCHEMA$;
    }
}
