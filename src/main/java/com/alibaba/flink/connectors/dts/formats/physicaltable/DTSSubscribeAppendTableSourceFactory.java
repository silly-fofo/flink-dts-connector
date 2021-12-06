package com.alibaba.flink.connectors.dts.formats.physicaltable;

import java.util.Collections;
import java.util.Set;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

public class DTSSubscribeAppendTableSourceFactory implements DeserializationFormatFactory {
    public static final String IDENTIFIER = "dts-append";

    public static final ConfigOption<Boolean> DTS_TABLE_NAME_IS_PATTERN = ConfigOptions.key("table.pattern")
        .booleanType()
        .defaultValue(false)
        .withDescription("The table names need to be collected.");

    public static final ConfigOption<String> DTS_NEEDED_TABLE = ConfigOptions.key("table.name")
        .noDefaultValue()
        .withDescription("The table names need to be collected.");

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
        DynamicTableFactory.Context context,
        ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                DynamicTableSource.Context context,
                DataType producedDataType) {

                final RowType rowType = (RowType)producedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                    context.createTypeInformation(producedDataType);
                return new DTSSubscribeAppendRowDataDeserializationSchema(rowType, rowDataTypeInfo,
                    formatOptions.get(DTS_NEEDED_TABLE), formatOptions.get(DTS_TABLE_NAME_IS_PATTERN));
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .build();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(DTS_NEEDED_TABLE);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.singleton(DTS_TABLE_NAME_IS_PATTERN);
    }
}
