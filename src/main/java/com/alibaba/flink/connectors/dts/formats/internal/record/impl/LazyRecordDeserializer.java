package com.alibaba.flink.connectors.dts.formats.internal.record.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.flink.connectors.dts.formats.internal.record.DatabaseInfo;
import com.alibaba.flink.connectors.dts.formats.internal.record.RecordField;
import com.alibaba.flink.connectors.dts.formats.internal.record.RecordIndexInfo;
import com.alibaba.flink.connectors.dts.formats.internal.record.value.*;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import com.alibaba.flink.connectors.dts.formats.internal.record.OperationType;
import com.alibaba.flink.connectors.dts.formats.internal.utils.ObjectNameUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/** operation deserializer. */
public class LazyRecordDeserializer {

    static OperationType[] operationDeserializers =
            new OperationType[OperationType.values().length];

    static {
        /** code: 0 name: com.alibaba.amp.any.common.record.formats.avro.Operation.INSERT */
        operationDeserializers[0] = OperationType.INSERT;

        /** code: 1 name: com.alibaba.amp.any.common.record.formats.avro.Operation.UPDATE */
        operationDeserializers[1] = OperationType.UPDATE;

        /** code: 2 name: com.alibaba.amp.any.common.record.formats.avro.Operation.DELETE */
        operationDeserializers[2] = OperationType.DELETE;

        /** code: 3 name: com.alibaba.amp.any.common.record.formats.avro.Operation.DDL */
        operationDeserializers[3] = OperationType.DDL;

        /** code: 4 name: com.alibaba.amp.any.common.record.formats.avro.Operation.BEGIN */
        operationDeserializers[4] = OperationType.BEGIN;

        /** code: 5 name: com.alibaba.amp.any.common.record.formats.avro.Operation.COMMIT */
        operationDeserializers[5] = OperationType.COMMIT;

        /** code: 6 name: com.alibaba.amp.any.common.record.formats.avro.Operation.ROLLBACK */
        operationDeserializers[6] = OperationType.ROLLBACK;

        /** code: 7 name: com.alibaba.amp.any.common.record.formats.avro.Operation.ABORT */
        operationDeserializers[7] = OperationType.ABORT;

        /** code: 8 name: com.alibaba.amp.any.common.record.formats.avro.Operation.HEARTBEAT */
        operationDeserializers[8] = OperationType.HEARTBEAT;

        /** code: 9 name: com.alibaba.amp.any.common.record.formats.avro.Operation.CHECKPOINT */
        operationDeserializers[9] = OperationType.CHECKPOINT;

        /** code: 10 name: com.alibaba.amp.any.common.record.formats.avro.Operation.COMMAND */
        operationDeserializers[10] = OperationType.COMMAND;

        /** code: 11 name: com.alibaba.amp.any.common.record.formats.avro.Operation.FILL */
        operationDeserializers[11] = OperationType.FILL;

        /** code: 12 name: com.alibaba.amp.any.common.record.formats.avro.Operation.FINISH */
        operationDeserializers[12] = OperationType.FINISH;

        /** code: 13 name: com.alibaba.amp.any.common.record.formats.avro.Operation.CONTROL */
        operationDeserializers[13] = OperationType.CONTROL;

        /** code: 14 name: com.alibaba.amp.any.common.record.formats.avro.Operation.RDB */
        operationDeserializers[14] = OperationType.RDB;

        /** code: 15 name: com.alibaba.amp.any.common.record.formats.avro.Operation.NOOP */
        operationDeserializers[15] = OperationType.NOOP;

        /** code: 16 name: com.alibaba.amp.any.common.record.formats.avro.Operation.INIT */
        operationDeserializers[16] = OperationType.INIT;
    }

    protected static DateTime deserializeDateTime(Decoder decoder, int sourceTypeCode)
            throws IOException {

        DateTime dateTime = new DateTime();
        int unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_YEAR);
            dateTime.setYear(decoder.readInt());
        } else {
            decoder.readNull();
        }

        unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_MONTH);
            dateTime.setMonth(decoder.readInt());
        } else {
            decoder.readNull();
        }

        unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_DAY);
            dateTime.setDay(decoder.readInt());
        } else {
            decoder.readNull();
        }

        unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_HOUR);
            dateTime.setHour(decoder.readInt());
        } else {
            decoder.readNull();
        }

        unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_MINITE);
            dateTime.setMinute(decoder.readInt());
        } else {
            decoder.readNull();
        }

        unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_SECOND);
            dateTime.setSecond(decoder.readInt());
        } else {
            decoder.readNull();
        }

        unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_NAONS);
            int naons = decoder.readInt();
            if (sourceTypeCode == SourceType.PostgreSQL.ordinal()
                    || sourceTypeCode == SourceType.MySQL.ordinal()) {
                naons *= 1000;
            }
            dateTime.setNaons(naons);
        } else {
            decoder.readNull();
        }
        return dateTime;
    }

    interface ValueDeserializer<T> {
        T deserialize(Decoder decoder, int sourceCode) throws IOException;
    }

    static ValueDeserializer<Value>[] valueDeserializers = new ValueDeserializer[13];

    static {

        /** code: 0 name: null */
        valueDeserializers[0] =
                (decoder, sourceTypeCode) -> {
                    decoder.readNull();
                    return null;
                };

        /** code: 1 name: com.alibaba.amp.any.common.record.formats.avro.Integer */
        valueDeserializers[1] =
                (decoder, sourceTypeCode) -> {
                    decoder.readInt();
                    return new IntegerNumeric(decoder.readString());
                };

        /** code: 2 name: com.alibaba.amp.any.common.record.formats.avro.Character */
        valueDeserializers[2] =
                (decoder, sourceTypeCode) -> {
                    String charset = decoder.readString();
                    ByteBuffer data = decoder.readBytes(null);
                    return new StringValue(data, charset);
                };

        /** code: 3 name: com.alibaba.amp.any.common.record.formats.avro.Decimal */
        valueDeserializers[3] =
                (decoder, sourceTypeCode) -> {
                    String text = decoder.readString();
                    decoder.readInt();
                    decoder.readInt();
                    try {
                        DecimalNumeric numeric = new DecimalNumeric(text);
                        return numeric;
                    } catch (NumberFormatException ex) {
                        return new SpecialNumeric(text);
                    }
                };

        /** code: 4 name: com.alibaba.amp.any.common.record.formats.avro.Float */
        valueDeserializers[4] =
                (decoder, sourceTypeCode) -> {
                    FloatNumeric numeric = new FloatNumeric(decoder.readDouble());
                    decoder.readInt();
                    decoder.readInt();
                    return numeric;
                };

        /** code: 5 name: com.alibaba.amp.any.common.record.formats.avro.Timestamp */
        valueDeserializers[5] =
                (decoder, sourceTypeCode) ->
                        new UnixTimestamp(decoder.readLong(), decoder.readInt());

        /** code: 6 name: com.alibaba.amp.any.common.record.formats.avro.DateTime */
        valueDeserializers[6] =
                (decoder, sourceTypeCode) -> deserializeDateTime(decoder, sourceTypeCode);

        /** code: 7 name: com.alibaba.amp.any.common.record.formats.avro.TimestampWithTimeZone */
        valueDeserializers[7] =
                (decoder, sourceTypeCode) -> {
                    DateTime dateTime = deserializeDateTime(decoder, sourceTypeCode);

                    String timeZone = decoder.readString();
                    dateTime.setSegments(DateTime.SEG_TIMEZONE);
                    if (SourceType.PostgreSQL.ordinal() == sourceTypeCode) {
                        timeZone = "GMT" + timeZone;
                    }
                    dateTime.setTimeZone(timeZone);
                    return dateTime;
                };

        /** code: 8 name: com.alibaba.amp.any.common.record.formats.avro.BinaryGeometry */
        valueDeserializers[8] =
                (decoder, sourceTypeCode) -> {
                    // skip type string
                    decoder.skipString();
                    return new WKBGeometry(decoder.readBytes(null));
                };

        /** code: 9 name: com.alibaba.amp.any.common.record.formats.avro.TextGeometry */
        valueDeserializers[9] =
                (decoder, sourceTypeCode) -> {
                    // skip type string
                    decoder.skipString();
                    return new WKTGeometry(decoder.readString());
                };

        /** code: 10 name: com.alibaba.amp.any.common.record.formats.avro.BinaryObject */
        valueDeserializers[10] =
                (decoder, sourceTypeCode) ->
                        new BinaryEncodingObject(
                                ObjectType.parse(decoder.readString().toUpperCase()),
                                decoder.readBytes(null));

        /** code: 11 name: com.alibaba.amp.any.common.record.formats.avro.TextObject */
        valueDeserializers[11] =
                (decoder, sourceTypeCode) ->
                        new TextEncodingObject(
                                ObjectType.parse(decoder.readString().toUpperCase()),
                                decoder.readString());

        /** code: 12 name: com.alibaba.amp.any.common.record.formats.avro.EmptyObject */
        valueDeserializers[12] =
                (decoder, sourceTypeCode) -> {
                    decoder.readEnum();
                    return new NoneValue();
                };
    }

    public static void deserializeHeader(BinaryDecoder decoder, LazyParseRecordImpl record)
            throws IOException {

        // skip version
        decoder.readInt();
        record.setId(decoder.readLong());
        record.setTimestamp(decoder.readLong());
        record.setSourcePosition(decoder.readString());
        record.setSourceSafePosition(decoder.readString());
        record.setTransactionId(decoder.readString());

        // skip safeSourcePosition
        Pair<Integer, String> dbInfoPair = deserializeSource(decoder);
        record.setSourceTypeCode(dbInfoPair.getKey());
        DatabaseInfo databaseInfo =
                new DatabaseInfo(
                        SourceType.values()[dbInfoPair.getKey()].name(), dbInfoPair.getValue());
        record.setSourceTypeAndVersion(
                Pair.of(databaseInfo.getDatabaseType(), databaseInfo.getVersion()));
        record.setOperationType(operationDeserializers[decoder.readEnum()]);

        // deserialize object names
        LazyRecordSchema recordSchema = null;
        int unionIndex = decoder.readIndex();
        if (0 == unionIndex) {
            decoder.readNull();
        } else if (1 == unionIndex) {
            // parse db/schema/tb name
            String objectName = decoder.readString();
            Triple<String, String, String> nameTriple = deserializeNameTriple(objectName);
            if (dbInfoPair.getKey() == SourceType.SQLServer.ordinal()) {
                recordSchema =
                        new LazyRecordSchema(
                                record,
                                nameTriple.toString(),
                                "[" + nameTriple.getLeft() + "]",
                                "[" + nameTriple.getMiddle() + "].[" + nameTriple.getRight() + "]");
            } else {
                recordSchema =
                        new LazyRecordSchema(
                                record,
                                nameTriple.toString(),
                                nameTriple.getLeft(),
                                nameTriple.getRight());
            }
            recordSchema.setDatabaseInfo(databaseInfo);
            record.setRecordSchema(recordSchema);
        }

        // skip Long list
        deserializeLongList(decoder, true);

        // deserialize tags
        Map<String, String> tags = deserializeMap(decoder);
        record.setExtendedProperty(tags);

        // try extract logical names
        if (null != recordSchema) {
            recordSchema.setLogicalDatabaseName(tags.get("l_db_name"));
            recordSchema.setLogicalTableName(tags.get("l_tb_name"));
        }
    }

    public static void deserializePayload(BinaryDecoder decoder, LazyParseRecordImpl record)
            throws IOException {

        // deserialize pk/uk info from tag field
        deserializeFieldListAndIndex(
                decoder, record, record.getExtendedProperty().get("pk_uk_info"));

        record.setBeforeImage(
                deserializeRowImage(decoder, record.getSchema(false), record.getSourceTypeCode()));
        record.setAfterImage(
                deserializeRowImage(decoder, record.getSchema(false), record.getSourceTypeCode()));

        if (!decoder.isEnd()) {
            record.setBornTimestamp(decoder.readLong());
        }
    }

    protected static Pair<Integer, String> deserializeSource(Decoder decoder) throws IOException {
        return Pair.of(decoder.readEnum(), decoder.readString());
    }

    protected static Triple<String, String, String> deserializeNameTriple(String mixedNames) {
        String dbName = null;
        String schemaName = null;
        String tableName = null;

        String[] dbPair = ObjectNameUtils.uncompressionObjectName(mixedNames);
        if (dbPair != null) {
            if (dbPair.length < 1 || dbPair.length > 3) {
                throw new RuntimeException(
                        "Invalid db table name pair for mixed [" + mixedNames + "]");
            } else {
                if (dbPair.length > 1) {
                    schemaName = dbPair[dbPair.length - 2];
                    tableName = dbPair[dbPair.length - 1];
                }
                dbName = dbPair[0];
            }
        }
        return Triple.of(dbName, schemaName, tableName);
    }

    protected static List<Long> deserializeLongList(Decoder decoder, boolean nullable)
            throws IOException {
        int longIndex = decoder.readIndex();
        if (1 == longIndex) {
            long chunkLen = (decoder.readArrayStart());
            List<Long> longList = new ArrayList<Long>();
            if (chunkLen > 0) {
                do {
                    for (int counter = 0; (counter < chunkLen); ++counter) {
                        longList.add(decoder.readLong());
                    }
                    chunkLen = decoder.arrayNext();
                } while (chunkLen > 0);
            }
            return longList;
        } else {
            decoder.readNull();
            return nullable ? null : new ArrayList<Long>(1);
        }
    }

    protected static Map<String, String> deserializeMap(Decoder decoder) throws IOException {
        Map<String, String> map = new LinkedHashMap<>();
        long chunkLen = (decoder.readArrayStart());
        if (chunkLen > 0) {
            do {
                for (int counter = 0; (counter < chunkLen); ++counter) {
                    String key = decoder.readString();
                    String value = decoder.readString();
                    map.put(key, value);
                }
                chunkLen = decoder.mapNext();
            } while (chunkLen > 0);
        }
        return map;
    }

    protected static Pair<Map<String, Boolean>, Map<String, Pair<Boolean, List<String>>>>
            deserializePkUkInfo(String text) throws IOException {
        if (StringUtils.isEmpty(text)) {
            return Pair.of(Collections.emptyMap(), Collections.emptyMap());
        }

        Map<String, Pair<Boolean, List<String>>> pkUkInfo = new LinkedHashMap<>();
        Map<String, Boolean> columnUkInfo = new HashMap<>();
        JSONObject jsonObject = JSON.parseObject(text);
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            String key = entry.getKey();
            JSONArray jsonArray = (JSONArray) entry.getValue();
            boolean isPrimaryKey = StringUtils.equals("PRIMARY", key);
            List<String> columns = new ArrayList<>();
            for (Object value : jsonArray) {
                String colName = (String) value;
                columns.add(colName);
                if (isPrimaryKey || (!columnUkInfo.containsKey(colName))) {
                    columnUkInfo.put(colName, isPrimaryKey);
                }
            }
            pkUkInfo.put(key, Pair.of(isPrimaryKey, columns));
        }
        return Pair.of(columnUkInfo, pkUkInfo);
    }

    protected static void deserializeFieldListAndIndex(
            Decoder decoder, LazyParseRecordImpl record, String pkukInfo) throws IOException {
        // parse pk/uk names
        Pair<Map<String, Boolean>, Map<String, Pair<Boolean, List<String>>>> pkUkInfo =
                deserializePkUkInfo(pkukInfo);

        LazyRecordSchema recordSchema = record.getSchema(false);
        int fieldIndex = decoder.readIndex();
        if (2 == fieldIndex) {

            List<RecordField> recordFields = deserializeFieldList(decoder, pkUkInfo.getKey());
            recordSchema.setRecordFields(recordFields);
            // build uk index infos
            Map<String, Pair<Boolean, List<String>>> pkUkInfoMap = pkUkInfo.getRight();
            for (Map.Entry<String, Pair<Boolean, List<String>>> entry : pkUkInfoMap.entrySet()) {

                boolean isPrimaryKey = entry.getValue().getLeft();
                RecordIndexInfo recordIndexInfo =
                        new RecordIndexInfo(
                                isPrimaryKey
                                        ? RecordIndexInfo.IndexType.PrimaryKey
                                        : RecordIndexInfo.IndexType.UniqueKey);
                for (String ukFieldName : entry.getValue().getRight()) {
                    recordIndexInfo.addField(
                            recordSchema
                                    .getField(ukFieldName, false)
                                    .orElseThrow(
                                            () ->
                                                    new RuntimeException(
                                                            ukFieldName
                                                                    + " not found in record ["
                                                                    + recordSchema
                                                                            .getFullQualifiedName()
                                                                    + "]")));
                }
                if (isPrimaryKey) {
                    recordSchema.setPrimaryIndexInfo(recordIndexInfo);
                } else {
                    recordSchema.addUniqueIndexInfo(recordIndexInfo);
                }
            }
        } else if (1 == fieldIndex) {
            decoder.readString();
        } else {
            decoder.readNull();
            LazyRecordSchema ddlRecordSchema = record.getSchema(false);
            if (OperationType.DDL == record.getOperationType()) {
                if (null == ddlRecordSchema) {
                    ddlRecordSchema = new LazyRecordSchema(record, null, null, null);
                    record.setRecordSchema(ddlRecordSchema);
                }
                RecordField ddlField = new SimplifiedRecordField("ddl", DefaultRawDataType.of(0));
                ddlRecordSchema.setRecordFields(Arrays.asList(ddlField));
            }
        }
    }

    protected static List<RecordField> deserializeFieldList(
            Decoder decoder, Map<String, Boolean> colPkUkInfo) throws IOException {
        long chunkLen = (decoder.readArrayStart());
        List<RecordField> recordFields = new ArrayList<>();
        int fieldPosition = 0;
        if (chunkLen > 0) {
            do {
                for (int counter = 0; (counter < chunkLen); ++counter) {
                    String fieldName = decoder.readString();
                    int fieldTypeNumber = decoder.readInt();
                    SimplifiedRecordField recordField =
                            new SimplifiedRecordField(
                                    fieldName, DefaultRawDataType.of(fieldTypeNumber));

                    Boolean isPkCol = colPkUkInfo.get(fieldName);
                    if (null != isPkCol) {
                        recordField.setUnique(true);
                        recordField.setPrimary(isPkCol);
                    }
                    recordField.setFieldPosition(fieldPosition);
                    recordFields.add(recordField);
                    fieldPosition++;
                }
                chunkLen = decoder.arrayNext();
            } while (chunkLen > 0);
        }
        return recordFields;
    }

    protected static DefaultRowImage deserializeRowImage(
            Decoder decoder, LazyRecordSchema recordSchema, int souceTypeCode) throws IOException {

        DefaultRowImage rowImage = null;
        int imageIndex = decoder.readIndex();
        if (2 == imageIndex) {
            long chunkLen = (decoder.readArrayStart());
            if (chunkLen > 0) {
                rowImage = new DefaultRowImage(recordSchema, recordSchema.getFieldCount(false));
                int index = 0;
                do {
                    for (int counter = 0; (counter < chunkLen); counter++) {
                        int type = (decoder.readIndex());
                        rowImage.setValue(
                                index,
                                valueDeserializers[type].deserialize(decoder, souceTypeCode));
                        ++index;
                    }
                    chunkLen = (decoder.arrayNext());
                } while (chunkLen > 0);
            }
        } else if (1 == imageIndex) {
            rowImage = new DefaultRowImage(recordSchema, recordSchema.getFieldCount(false));
            rowImage.setValue(0, new StringValue(decoder.readString()));
        } else {
            decoder.readNull();
            return null;
        }
        return rowImage;
    }
}
