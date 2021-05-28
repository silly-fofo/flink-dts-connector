package com.alibaba.flink.connectors.dts.table;


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.Serializable;

public class DynamicDtsDeserializationSchema implements KafkaDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<RowData> valueDeserialization;

    private final boolean hasMetadata;

    private final TypeInformation<RowData> producedTypeInfo;

    private final OutputProjectionCollector outputCollector;


    public DynamicDtsDeserializationSchema(int physicalArity,
                                           DeserializationSchema<RowData> valueDeserialization,
                                           int[] valueProjection,
                                           boolean hasMetadata,
                                           DtsDynamicSource.ReadableMetadata.MetadataConverter[] metadataConverters,
                                           TypeInformation<RowData> producedTypeInfo) {
        this.valueDeserialization = valueDeserialization;
        this.hasMetadata = hasMetadata;
        this.outputCollector =
                new OutputProjectionCollector(
                        physicalArity,
                        valueProjection,
                        metadataConverters);
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public RowData deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        throw new IllegalStateException("A collector is required for deserializing.");
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector)
            throws Exception {
        // project output while emitting values
        outputCollector.inputRecord = record;
        outputCollector.outputCollector = collector;

        valueDeserialization.deserialize(record.value(), outputCollector);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Emits a row with key, value, and metadata fields.
     *
     * <p>The collector is able to handle the following kinds of keys:
     *
     * <ul>
     *   <li>No key is used.
     *   <li>A key is used.
     *   <li>The deserialization schema emits multiple keys.
     *   <li>Keys and values have overlapping fields.
     *   <li>Keys are used and value is null.
     * </ul>
     */
    private static final class OutputProjectionCollector
            implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final int physicalArity;

        private final int[] valueProjection;

        private final DtsDynamicSource.ReadableMetadata.MetadataConverter[] metadataConverters;

        private transient ConsumerRecord<?, ?> inputRecord;

        private transient Collector<RowData> outputCollector;

        OutputProjectionCollector(
                int physicalArity,
                int[] valueProjection,
                DtsDynamicSource.ReadableMetadata.MetadataConverter[] metadataConverters) {
            this.physicalArity = physicalArity;
            this.valueProjection = valueProjection;
            this.metadataConverters = metadataConverters;
        }

        @Override
        public void collect(RowData physicalValueRow) {
            emitRow(null, (GenericRowData) physicalValueRow);
        }

        @Override
        public void close() {
            // nothing to do
        }

        private void emitRow(
                @Nullable GenericRowData physicalKeyRow,
                @Nullable GenericRowData physicalValueRow) {
            final RowKind rowKind;

            rowKind = physicalValueRow.getRowKind();

            final int metadataArity = metadataConverters.length;
            final GenericRowData producedRow =
                    new GenericRowData(rowKind, physicalArity + metadataArity);

            if (physicalValueRow != null) {
                for (int valuePos = 0; valuePos < valueProjection.length; valuePos++) {
                    producedRow.setField(
                            valueProjection[valuePos], physicalValueRow.getField(valuePos));
                }
            }

            for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
                producedRow.setField(
                        physicalArity + metadataPos,
                        metadataConverters[metadataPos].read(inputRecord));
            }

            outputCollector.collect(producedRow);
        }
    }
}
