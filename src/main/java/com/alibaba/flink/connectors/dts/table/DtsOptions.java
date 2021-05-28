package com.alibaba.flink.connectors.dts.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

public class DtsOptions {

    private DtsOptions() {}

    public static final ConfigOption<String> DTS_BOOTSTRAP_SERVERS =
            ConfigOptions.key("dts.server")
                    .noDefaultValue()
                    .withDescription("Required dts server connection string");

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic")
                    .noDefaultValue()
                    .withDescription(
                            "Topic names from which the table is read. Either 'topic' or 'topic-pattern' must be set for source. ");

    public static final ConfigOption<String> DTS_SID =
            ConfigOptions.key("dts.sid")
                    .noDefaultValue()
                    .withDescription(
                            "Required consumer channel id in DTS consumer.");

    public static final ConfigOption<String> DTS_GROUP =
            ConfigOptions.key("dts.sid")
                    .noDefaultValue()
                    .withDescription(
                            "Required consumer group in DTS consumer.");

    public static final ConfigOption<String> DTS_USER =
            ConfigOptions.key("dts.user")
                    .noDefaultValue()
                    .withDescription(
                            "Required consumer user name in DTS consumer");

    public static final ConfigOption<String> DTS_PASSWORD =
            ConfigOptions.key("dts.password")
                    .noDefaultValue()
                    .withDescription(
                            "Required consumer password in DTS consumer");

    public static final ConfigOption<String> DTS_CHECKPOINT =
            ConfigOptions.key("dts.checkpoint")
                    .noDefaultValue()
                    .withDescription(
                            "Required consumer start checkpoint in DTS consumer");

    /**
     * Creates an array of indices that determine which physical fields of the table schema to
     * include in the value format.
     */
    public static int[] createValueFormatProjection(
            ReadableConfig options, DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        return physicalFields.toArray();
    }
}
