package com.alibaba.flink.connectors.dts.table;

import com.alibaba.flink.connectors.dts.FlinkDtsKafkaConsumer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.flink.table.connector.source.SourceProvider;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** A version-agnostic Dts {@link ScanTableSource}. */
@Internal
public class DtsDynamicSource implements ScanTableSource, SupportsReadingMetadata, SupportsWatermarkPushDown {
    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    /** Watermark strategy that is used to generate per-partition watermark. */
    protected @Nullable
    WatermarkStrategy<RowData> watermarkStrategy;

    // --------------------------------------------------------------------------------------------
    // Format attributes
    // --------------------------------------------------------------------------------------------

    private static final String VALUE_METADATA_PREFIX = "value.";

    /** Format for decoding values from Kafka. */
    protected final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    /** Indices that determine the value fields and the target position in the produced row. */
    protected final int[] valueProjection;

    protected final String server;
    protected final String topic;
    protected final String sid;
    protected final String user;
    protected final String password;
    protected final long checkpoint;

    /** Data type to configure the formats. */
    protected final DataType physicalDataType;

    public DtsDynamicSource(String server, String topic, String sid, String user, String password, long checkpoint,
                            DataType physicalDataType,
                            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
                            int[] valueProjection) {

        this.server = server;
        this.topic = topic;
        this.sid = sid;
        this.user = user;
        this.password = password;
        this.checkpoint = checkpoint;

        this.physicalDataType = physicalDataType;
        this.producedDataType = physicalDataType;

        this.metadataKeys = Collections.emptyList();

        this.valueDecodingFormat =
                Preconditions.checkNotNull(
                        valueDecodingFormat, "Value decoding format must not be null.");

        this.valueProjection =
                Preconditions.checkNotNull(valueProjection, "Value projection must not be null.");
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return valueDecodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {

        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(context, valueDecodingFormat, valueProjection);

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        final FlinkDtsKafkaConsumer<RowData> dtsConsumer =
                createDtsConsumer(valueDeserialization, producedTypeInfo);

        return SourceFunctionProvider.of(dtsConsumer, false);
    }

    private DeserializationSchema<RowData> createDeserialization(ScanContext context,
                                                                 DecodingFormat<DeserializationSchema<RowData>> format,
                                                                 int[] projection) {
        if (format == null) {
            return null;
        }

        DataType physicalFormatDataType =
                DataTypeUtils.projectRow(this.physicalDataType, projection);

        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    private FlinkDtsKafkaConsumer createDtsConsumer(
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {

        final ReadableMetadata.MetadataConverter[] metadataConverters =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .map(m -> m.converter)
                        .toArray(ReadableMetadata.MetadataConverter[]::new);

        // check if connector metadata is used at all
        final boolean hasMetadata = metadataKeys.size() > 0;

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity =
                producedDataType.getChildren().size() - metadataKeys.size();

        // adjust value format projection to include value format's metadata columns at the end
        final int[] adjustedValueProjection =
                IntStream.concat(
                        IntStream.of(valueProjection),
                        IntStream.range(
                                valueProjection.length,
                                adjustedPhysicalArity))
                        .toArray();

        final KafkaDeserializationSchema<RowData> kafkaDeserializer =
                new DynamicDtsDeserializationSchema(
                        adjustedPhysicalArity,
                        valueDeserialization,
                        adjustedValueProjection,
                        hasMetadata,
                        metadataConverters,
                        producedTypeInfo);


        FlinkDtsKafkaConsumer dtsConsumer = new FlinkDtsKafkaConsumer(this.server, this.topic, this.sid, this.user,
                                                this.password, this.checkpoint, kafkaDeserializer);

        return dtsConsumer;
    }

    @Override
    public DynamicTableSource copy() {
        final DtsDynamicSource copy =
                new DtsDynamicSource(
                        server,
                        topic,
                        sid,
                        user,
                        password,
                        checkpoint,
                        physicalDataType,
                        valueDecodingFormat,
                        valueProjection);
        copy.producedDataType = producedDataType;
        copy.metadataKeys = metadataKeys;
        copy.watermarkStrategy = watermarkStrategy;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return null;
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();

        // according to convention, the order of the final row must be
        // PHYSICAL + FORMAT METADATA + CONNECTOR METADATA
        // where the format metadata has highest precedence

        // add value format metadata with prefix
        valueDecodingFormat
                .listReadableMetadata()
                .forEach((key, value) -> metadataMap.put(VALUE_METADATA_PREFIX + key, value));

        // add connector metadata
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.putIfAbsent(m.key, m.dataType));

        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        // separate connector and format metadata
        final List<String> formatMetadataKeys =
                metadataKeys.stream()
                        .filter(k -> k.startsWith(VALUE_METADATA_PREFIX))
                        .collect(Collectors.toList());
        final List<String> connectorMetadataKeys = new ArrayList<>(metadataKeys);
        connectorMetadataKeys.removeAll(formatMetadataKeys);

        // push down format metadata
        final Map<String, DataType> formatMetadata = valueDecodingFormat.listReadableMetadata();
        if (formatMetadata.size() > 0) {
            final List<String> requestedFormatMetadataKeys =
                    formatMetadataKeys.stream()
                            .map(k -> k.substring(VALUE_METADATA_PREFIX.length()))
                            .collect(Collectors.toList());
            valueDecodingFormat.applyReadableMetadata(requestedFormatMetadataKeys);
        }

        this.metadataKeys = connectorMetadataKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum ReadableMetadata {
        TOPIC(
                "topic",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return StringData.fromString(record.topic());
                    }
                }),

        PARTITION(
                "partition",
                DataTypes.INT().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return record.partition();
                    }
                }),

        HEADERS(
                "headers",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
                        .notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        final Map<StringData, byte[]> map = new HashMap<>();
                        for (Header header : record.headers()) {
                            map.put(StringData.fromString(header.key()), header.value());
                        }
                        return new GenericMapData(map);
                    }
                }),

        OFFSET(
                "offset",
                DataTypes.BIGINT().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return record.offset();
                    }
                }),

        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return TimestampData.fromEpochMillis(record.timestamp());
                    }
                }),

        TIMESTAMP_TYPE(
                "timestamp-type",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record) {
                        return StringData.fromString(record.timestampType().toString());
                    }
                });

        final String key;

        final DataType dataType;

        final MetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }

        // --------------------------------------------------------------------------------------------

        interface MetadataConverter extends Serializable {
            Object read(ConsumerRecord<?, ?> record);
        }
    }
}
