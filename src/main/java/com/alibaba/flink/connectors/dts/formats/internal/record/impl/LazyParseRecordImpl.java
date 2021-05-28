package com.alibaba.flink.connectors.dts.formats.internal.record.impl;

import com.alibaba.flink.connectors.dts.formats.internal.common.NullableOptional;
import com.alibaba.flink.connectors.dts.formats.internal.record.DtsRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang3.tuple.Pair;
import com.alibaba.flink.connectors.dts.formats.internal.record.OperationType;
import com.alibaba.flink.connectors.dts.formats.internal.record.RowImage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** an lazy implement for DtsRecord. */
public class LazyParseRecordImpl implements DtsRecord {

    private byte[] rawValue;
    private volatile boolean initHeader = false;
    private BinaryDecoder binaryDecoder;
    private long id;
    private long timestamp;
    private String sourcePosition;
    private String sourceSafePosition;
    private String transactionId;
    private OperationType operationType;
    private Pair<String, String> sourceTypeAndVersion;
    private int sourceTypeCode;
    private LazyRecordSchema recordSchema;

    private volatile boolean initPayload = false;
    private RowImage beforeImage;
    private RowImage afterImage;

    private Map<String, String> extendedProperty;
    private NullableOptional<Boolean> keyChanged = NullableOptional.empty();
    // for delete, the pk and uk value list in before images
    private List<List<String>> prevKeys;
    // for insert and update,  the pk and uk value list in after images
    private List<List<String>> nextKeys;
    private long size;

    private long bornTimestamp;

    public LazyParseRecordImpl(byte[] recordData) {
        this.rawValue = recordData;
        this.size = recordData.length;
    }

    protected void initHeaderIfNeeded() {
        if (!this.initHeader) {
            synchronized (this) {
                if (!this.initHeader) {
                    this.binaryDecoder = DecoderFactory.get().binaryDecoder(this.rawValue, null);
                    try {
                        LazyRecordDeserializer.deserializeHeader(this.binaryDecoder, this);
                    } catch (IOException ex) {
                        throw new RuntimeException("deserialize value head error", ex);
                    }
                    this.initHeader = true;
                }
            }
        }
    }

    protected void initPayloadIfNeeded() {
        if (!this.initPayload) {
            initHeaderIfNeeded();
            synchronized (this) {
                if (!this.initPayload) {
                    try {
                        LazyRecordDeserializer.deserializePayload(this.binaryDecoder, this);
                    } catch (IOException ex) {
                        throw new RuntimeException("deserialize value body error", ex);
                    }
                    this.initPayload = true;
                    this.binaryDecoder = null;
                }
            }
        }
    }

    @Override
    public long getId() {
        initHeaderIfNeeded();
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setSourcePosition(String sourcePosition) {
        this.sourcePosition = sourcePosition;
    }

    @Override
    public String getTransactionId() {
        initHeaderIfNeeded();
        return this.transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public long getTimestamp() {
        initHeaderIfNeeded();
        return this.timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public OperationType getOperationType() {
        initHeaderIfNeeded();
        return this.operationType;
    }

    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }

    @Override
    public LazyRecordSchema getSchema() {
        return this.getSchema(true);
    }

    public LazyRecordSchema getSchema(boolean load) {
        if (load) {
            initHeaderIfNeeded();
            if (null == this.recordSchema) {
                initPayloadIfNeeded();
            }
        }
        return this.recordSchema;
    }

    public void setRecordSchema(LazyRecordSchema recordSchema) {
        this.recordSchema = recordSchema;
    }

    public void setBeforeImage(RowImage beforeImage) {
        this.beforeImage = beforeImage;
    }

    @Override
    public RowImage getBeforeImage() {
        initPayloadIfNeeded();
        return this.beforeImage;
    }

    public void setAfterImage(RowImage afterImage) {
        this.afterImage = afterImage;
    }

    @Override
    public RowImage getAfterImage() {
        initPayloadIfNeeded();
        return this.afterImage;
    }

    @Override
    public Set<String> getRawFieldNames() {
        List<String> fieldNames = this.recordSchema.getFieldNames();
        return fieldNames.stream().collect(Collectors.toSet());
    }

    public void setExtendedProperty(Map<String, String> extendedProperty) {
        this.extendedProperty = extendedProperty;
    }

    @Override
    public Map<String, String> getExtendedProperty() {
        initHeaderIfNeeded();
        return this.extendedProperty;
    }

    @Override
    public long size() {
        return size;
    }

    /**
     * This function is used to truncated the fully id to lower 31 bits. The purpose of it is to
     * make writer2.0 happy. When all writer2.0 are replaced, we can use getId() directly.
     */
    private int getIdLowerValue() {
        final int positiveIntMask = 0x7FFFFFFF;
        return (int) (getId() & positiveIntMask);
    }

    @Override
    public String getCheckpoint() {
        final String itemSeparator = "@";

        return String.join(
                itemSeparator,
                "0",
                Long.toString(getIdLowerValue()),
                this.sourcePosition,
                Long.toString(TimeUnit.SECONDS.toMillis(getTimestamp())));
    }

    public byte[] getRawValue() {
        return rawValue;
    }

    public String getSourceSafePosition() {
        initHeaderIfNeeded();
        return this.sourceSafePosition;
    }

    public void setSourceSafePosition(String sourceSafePosition) {
        this.sourceSafePosition = sourceSafePosition;
    }

    public int getSourceTypeCode() {
        return sourceTypeCode;
    }

    public void setSourceTypeCode(int sourceTypeCode) {
        this.sourceTypeCode = sourceTypeCode;
    }

    @Override
    public Pair<String, Object> getRecordRawData() {
        return Pair.of("byte[]", this.rawValue);
    }

    public void setSourceTypeAndVersion(Pair<String, String> sourceTypeAndVersion) {
        this.sourceTypeAndVersion = sourceTypeAndVersion;
    }

    @Override
    public Pair<String, String> getSourceTypeAndVersion() {
        initHeaderIfNeeded();
        return this.sourceTypeAndVersion;
    }

    @Override
    public String toString() {
        return "LazyParseRecord {"
                + "operationType ["
                + getOperationType()
                + "], "
                + "checkpoint ["
                + getCheckpoint()
                + "]}";
    }

    @Override
    public long getBornTimestamp() {
        initPayloadIfNeeded();

        if (bornTimestamp > 0) {
            return bornTimestamp;
        }

        return getTimestamp();
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }
}
