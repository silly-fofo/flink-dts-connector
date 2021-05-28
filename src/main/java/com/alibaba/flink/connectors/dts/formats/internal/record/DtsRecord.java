package com.alibaba.flink.connectors.dts.formats.internal.record;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;
import java.util.Set;

/** DtsRecord replsent all database one record. */
public interface DtsRecord {

    /** Get the record unique id. */
    long getId();

    /** Get transaction id that current record belongs to. */
    String getTransactionId();

    /**
     * Get the epoch timestamp of current record produced in transaction. in some case, this should
     * be rewrote according the commit timestamp.
     */
    long getTimestamp();

    /** Get the epoch timestamp of current record is born. */
    long getBornTimestamp();

    /** Get checkpoint of current record. */
    default String getCheckpoint() {
        return "";
    }

    /** Get the operation which causes current record. */
    OperationType getOperationType();

    /** Get the schema of current record data. */
    RecordSchema getSchema();

    /** Get the before row image of current record. */
    RowImage getBeforeImage();

    /**
     * Get the after row image of current record.
     *
     * @return
     */
    RowImage getAfterImage();

    /**
     * Returns a Set that contains the names of all of the fields that are present in the Record,
     * regardless of whether or not those fields are contained in the schema. To determine which
     * fields exist in the Schema, use {@link #getSchema()}.{@link RecordSchema#getFieldNames()
     * getFieldNames()} instead.
     *
     * @return a Set that contains the names of all of the fields that are present in the Record
     */
    Set<String> getRawFieldNames();

    /*
     * Get extended properties, that should not be record data, such as the tag info.
     */
    Map<String, String> getExtendedProperty();

    /*
     * Get the size in bytes of current record.
     */
    long size();

    /*
     * Get the record raw data type and record raw data of the Record, such as avroRecord in increment use environment
     * @return
     */
    default Pair<String, Object> getRecordRawData() {
        return null;
    }

    /*
     * Get info where the record comes from
     *
     * @return pair.left is the source name, pair.right is the source version
     */
    default Pair<String, String> getSourceTypeAndVersion() {
        return null;
    }

    /*
     * Get the record's sequence number in the transaction
     * @return
     */
    default long getTransSeq() {
        return 0;
    }
}
