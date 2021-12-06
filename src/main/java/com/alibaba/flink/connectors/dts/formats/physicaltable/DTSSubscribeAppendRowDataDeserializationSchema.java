package com.alibaba.flink.connectors.dts.formats.physicaltable;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.regex.Pattern;

import com.alibaba.flink.connectors.dts.formats.internal.common.NullableOptional;
import com.alibaba.flink.connectors.dts.formats.internal.record.EtlRow;
import com.alibaba.flink.connectors.dts.formats.internal.record.OperationType;
import com.alibaba.flink.connectors.dts.formats.internal.record.impl.LazyParseRecordImpl;
import com.alibaba.flink.connectors.dts.formats.internal.record.value.BinaryEncodingObject;
import com.alibaba.flink.connectors.dts.formats.internal.record.value.DateTime;
import com.alibaba.flink.connectors.dts.formats.internal.record.value.DecimalNumeric;
import com.alibaba.flink.connectors.dts.formats.internal.record.value.FloatNumeric;
import com.alibaba.flink.connectors.dts.formats.internal.record.value.IntegerNumeric;
import com.alibaba.flink.connectors.dts.formats.internal.record.value.UnixTimestamp;
import com.alibaba.flink.connectors.dts.formats.internal.record.value.Value;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

/**
 * @author piccaboo
 * @date 2021/12/01
 */
public class DTSSubscribeAppendRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private final String tableName;

    /**
     * TypeInformation of the produced {@link RowData}. *
     */
    private final TypeInformation<RowData> resultTypeInfo;

    /**
     * Runtime converter that converts into objects of Flink SQL internal data
     * structures. *
     */
    private final DeserializationRuntimeConverter runtimeConverter;

    private final RowType rowType;

    private final boolean isPattern;

    private final static String SCHEMA_COLUMN = "dts_etl_schema_db_table";

    private final static String OPERATION_TYPE = "dts_etl_operation_type";

    private final static String DB_LOG_TIME = "dts_etl_db_log_time";

    private boolean haveOperationType = false;

    public DTSSubscribeAppendRowDataDeserializationSchema(
        RowType rowType,
        TypeInformation<RowData> resultTypeInfo,
        String tableName,
        boolean isPattern
    ) {
        this.tableName = tableName;
        this.rowType = rowType;
        this.resultTypeInfo = resultTypeInfo;
        this.isPattern = isPattern;
        this.runtimeConverter = createConverter(rowType);
        this.haveOperationType = rowType.getFieldNames().contains(OPERATION_TYPE);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        throw new RuntimeException(
            "Please invoke DTSSubscribeAppendRowDataDeserializationSchema#deserialize(byte[], Collector<RowData>) "
                + "instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        LazyParseRecordImpl record = new LazyParseRecordImpl(message);

        if (!OperationType.INSERT.equals(record.getOperationType()) && !OperationType.UPDATE.equals(
            record.getOperationType()) && !OperationType.DELETE.equals(record.getOperationType())) {
            return;
        }
        if (isPattern) {
            if (!Pattern.matches(tableName, record.getSchema().getFullQualifiedName().get())) {
                return;
            }
        } else if (!tableName.equals(record.getSchema().getFullQualifiedName().get())) {
            return;
        }

        if (record.getOperationType() == OperationType.INSERT) {
            GenericRowData insert = extractAfterRow(record, RowKind.INSERT);
            insert.setRowKind(RowKind.INSERT);
            out.collect(insert);
        } else if (record.getOperationType() == OperationType.DELETE) {
            if (haveOperationType) {
                GenericRowData delete = extractBeforeRow(record, RowKind.DELETE);
                delete.setRowKind(RowKind.INSERT);
                out.collect(delete);
            }
        } else {
            if (haveOperationType) {
                GenericRowData before = extractBeforeRow(record, RowKind.UPDATE_BEFORE);
                before.setRowKind(RowKind.INSERT);
                out.collect(before);
            }

            GenericRowData after = extractAfterRow(record, RowKind.UPDATE_AFTER);
            after.setRowKind(RowKind.INSERT);
            out.collect(after);
        }
    }

    private GenericRowData extractAfterRow(LazyParseRecordImpl record, RowKind rowKind) {
        return (GenericRowData)runtimeConverter.convert(
            new EtlRow(record.getAfterImage(), record.getSchema().getFullQualifiedName().get(), rowKind.name(),
                record.getBornTimestamp()));
    }

    private GenericRowData extractBeforeRow(LazyParseRecordImpl record, RowKind rowKind) {
        return (GenericRowData)runtimeConverter.convert(
            new EtlRow(record.getBeforeImage(), record.getSchema().getFullQualifiedName().get(), rowKind.name(),
                record.getBornTimestamp()));
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    /**
     * Runtime converter that converts objects of Debezium into objects of Flink Table & SQL
     * internal data structures.
     */
    @FunctionalInterface
    private interface DeserializationRuntimeConverter extends Serializable {
        Object convert(Object dbzObj);
    }

    /**
     * Creates a runtime converter which is null safe.
     */
    private DeserializationRuntimeConverter createConverter(LogicalType type) {
        return wrapIntoNullableConverter(createNotNullConverter(type));
    }

    /**
     * Creates a runtime converter which assuming input object is not null.
     */
    private DeserializationRuntimeConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (dbzObj) -> null;
            case BOOLEAN:
                return this::convertToBoolean;
            case TINYINT:
                return (dbzObj) -> Byte.parseByte(dbzObj.toString());
            case SMALLINT:
                return (dbzObj) -> Short.parseShort(dbzObj.toString());
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return this::convertToInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return this::convertToLong;
            case DATE:
                return this::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return this::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this::convertToTimestamp;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this::convertToLocalTimeZoneTimestamp;
            case FLOAT:
                return this::convertToFloat;
            case DOUBLE:
                return this::convertToDouble;
            case CHAR:
            case VARCHAR:
                return this::convertToString;
            case BINARY:
            case VARBINARY:
                return this::convertToBinary;
            case DECIMAL:
                return createDecimalConverter((DecimalType)type);
            case ROW:
                return createRowConverter((RowType)type);
            case ARRAY:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private TimestampData convertToTimestamp(Object dbzObj) {
        if (dbzObj instanceof IntegerNumeric) {
            return TimestampData.fromEpochMillis(((IntegerNumeric)dbzObj).getData().longValue());
        } else if (dbzObj instanceof UnixTimestamp) {
            Timestamp timestamp = ((UnixTimestamp)dbzObj).toJdbcTimestamp();
            return TimestampData.fromEpochMillis(timestamp.getTime(), timestamp.getNanos());
        } else if (dbzObj instanceof DateTime) {
            DateTime dateTime = (DateTime)dbzObj;
            return TimestampData.fromLocalDateTime(LocalDateTime
                .of(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay(), dateTime.getHour(),
                    dateTime.getMinute(), dateTime.getSecond(), dateTime.getNaons()));
        }
        throw new IllegalArgumentException(
            "Unable to convert to TimestampData from unexpected value '"
                + dbzObj
                + "' of type "
                + dbzObj.getClass().getName());
    }

    private boolean convertToBoolean(Object dbzObj) {
        if (dbzObj instanceof IntegerNumeric) {
            return ((IntegerNumeric)dbzObj).getData().longValue() > 0;
        } else {
            return Boolean.parseBoolean(dbzObj.toString());
        }
    }

    private int convertToDate(Object dbzObj) {
        if (dbzObj instanceof DateTime) {
            DateTime dateTime = (DateTime)dbzObj;
            return (int)LocalDate.of(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay()).toEpochDay();
        }
        throw new IllegalArgumentException(
            "Unable to convert to LocalDate from unexpected value '"
                + dbzObj
                + "' of type "
                + dbzObj.getClass().getName());
    }

    private int convertToInt(Object dbzObj) {
        if (dbzObj instanceof IntegerNumeric) {
            return ((IntegerNumeric)dbzObj).getData().intValue();
        } else {
            return Integer.parseInt(dbzObj.toString());
        }
    }

    private long convertToLong(Object dbzObj) {
        if (dbzObj instanceof IntegerNumeric) {
            return ((IntegerNumeric)dbzObj).getData().longValue();
        } else {
            return Long.parseLong(dbzObj.toString());
        }
    }

    private int convertToTime(Object dbzObj) {
        if (dbzObj instanceof DateTime) {
            DateTime dateTime = (DateTime)dbzObj;
            return LocalTime.of(dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond()).toSecondOfDay() * 1000;
        }
        // get number of milliseconds of the day
        throw new IllegalArgumentException(
            "Unable to convert to Time from unexpected value '"
                + dbzObj
                + "' of type "
                + dbzObj.getClass().getName());
    }

    private TimestampData convertToLocalTimeZoneTimestamp(Object dbzObj) {
        if (dbzObj instanceof IntegerNumeric) {
            return TimestampData.fromEpochMillis(((IntegerNumeric)dbzObj).getData().longValue());
        } else if (dbzObj instanceof UnixTimestamp) {
            Timestamp timestamp = ((UnixTimestamp)dbzObj).toJdbcTimestamp();
            return TimestampData.fromEpochMillis(timestamp.getTime(), timestamp.getNanos() / 1000);
        } else if (dbzObj instanceof DateTime) {
            DateTime dateTime = (DateTime)dbzObj;
            return TimestampData.fromLocalDateTime(LocalDateTime
                .of(dateTime.getYear(), dateTime.getMonth(), dateTime.getDay(), dateTime.getHour(),
                    dateTime.getMinute(), dateTime.getSecond(), dateTime.getNaons()));
        }
        throw new IllegalArgumentException(
            "Unable to convert to TimestampData from unexpected value '"
                + dbzObj
                + "' of type "
                + dbzObj.getClass().getName());
    }

    private float convertToFloat(Object dbzObj) {
        if (dbzObj instanceof FloatNumeric) {
            return ((FloatNumeric)dbzObj).getData().floatValue();
        } else if (dbzObj instanceof Float) {
            return (float)dbzObj;
        } else if (dbzObj instanceof Double) {
            return ((Double)dbzObj).floatValue();
        } else {
            return Float.parseFloat(dbzObj.toString());
        }
    }

    private double convertToDouble(Object dbzObj) {
        if (dbzObj instanceof FloatNumeric) {
            return ((FloatNumeric)dbzObj).getData();
        }
        if (dbzObj instanceof Float) {
            return (double)dbzObj;
        } else if (dbzObj instanceof Double) {
            return (double)dbzObj;
        } else {
            return Double.parseDouble(dbzObj.toString());
        }
    }

    private StringData convertToString(Object dbzObj) {
        return StringData.fromString(dbzObj.toString());
    }

    private byte[] convertToBinary(Object dbzObj) {
        if (dbzObj instanceof BinaryEncodingObject) {
            return ((BinaryEncodingObject)dbzObj).getData().array();
        } else if (dbzObj instanceof byte[]) {
            return (byte[])dbzObj;
        } else if (dbzObj instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer)dbzObj;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            throw new UnsupportedOperationException(
                "Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
        }
    }

    private DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return (dbzObj) -> {
            BigDecimal bigDecimal;
            if (dbzObj instanceof DecimalNumeric) {
                bigDecimal = ((DecimalNumeric)dbzObj).getData();
            } else if (dbzObj instanceof String) {
                // decimal.handling.mode=string
                bigDecimal = new BigDecimal((String)dbzObj);
            } else {
                bigDecimal = new BigDecimal(dbzObj.toString());
            }
            return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
        };
    }

    private DeserializationRuntimeConverter createRowConverter(RowType rowType) {
        final DeserializationRuntimeConverter[] fieldConverters =
            rowType.getFields().stream()
                .map(RowType.RowField::getType)
                .map(this::createConverter)
                .toArray(DeserializationRuntimeConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return (dbzObj) -> {
            EtlRow record = (EtlRow)dbzObj;
            int arity = fieldNames.length;
            GenericRowData row = new GenericRowData(arity);
            for (int i = 0; i < arity; i++) {
                String fieldName = fieldNames[i];

                if (fieldName.equalsIgnoreCase(SCHEMA_COLUMN)) {
                    row.setField(i, StringData.fromString(record.getTableName()));
                } else if (fieldName.equalsIgnoreCase(OPERATION_TYPE)) {
                    row.setField(i, StringData.fromString(record.getOperationType()));
                } else if (fieldName.equalsIgnoreCase(DB_LOG_TIME)) {
                    row.setField(i, record.getTimestamp());
                } else {
                    NullableOptional<Value> v = record.getRowImage().getValue(fieldName);
                    Object fieldValue = v.isPresent() ? v.get() : null;
                    Object convertedField = convertField(fieldConverters[i], fieldValue);
                    row.setField(i, convertedField);
                }
            }
            return row;
        };
    }

    private Object convertField(
        DeserializationRuntimeConverter fieldConverter, Object fieldValue) {
        if (fieldValue == null) {
            return null;
        } else {
            return fieldConverter.convert(fieldValue);
        }
    }

    private DeserializationRuntimeConverter wrapIntoNullableConverter(
        DeserializationRuntimeConverter converter) {
        return (dbzObj) -> {
            if (dbzObj == null) {
                return null;
            }
            return converter.convert(dbzObj);
        };
    }

}

