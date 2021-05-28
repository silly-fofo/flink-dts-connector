package com.alibaba.flink.connectors.dts.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static com.alibaba.flink.connectors.dts.table.DtsOptions.DTS_BOOTSTRAP_SERVERS;
import static com.alibaba.flink.connectors.dts.table.DtsOptions.TOPIC;
import static com.alibaba.flink.connectors.dts.table.DtsOptions.DTS_SID;
import static com.alibaba.flink.connectors.dts.table.DtsOptions.DTS_USER;
import static com.alibaba.flink.connectors.dts.table.DtsOptions.DTS_PASSWORD;
import static com.alibaba.flink.connectors.dts.table.DtsOptions.DTS_CHECKPOINT;

import static com.alibaba.flink.connectors.dts.table.DtsOptions.createValueFormatProjection;


public class DtsDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "dts";

    // Prefix for Kafka specific properties.
    public static final String PROPERTIES_PREFIX = "dts.";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DTS_BOOTSTRAP_SERVERS);
        options.add(TOPIC);
        options.add(DTS_SID);
        options.add(DTS_USER);
        options.add(DTS_PASSWORD);
        options.add(DTS_CHECKPOINT);

        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public DtsDynamicSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig tableOptions = helper.getOptions();

        //dts consumer properties
        String server = String.valueOf(tableOptions.get(DTS_BOOTSTRAP_SERVERS));
        String topic = String.valueOf(tableOptions.get(TOPIC));
        String sid = String.valueOf(tableOptions.get(DTS_SID));
        String user = String.valueOf(tableOptions.get(DTS_USER));
        String password = String.valueOf(tableOptions.get(DTS_PASSWORD));
        long checkpoint = Long.parseLong(tableOptions.get(DTS_CHECKPOINT));


        final DataType physicalDataType =
                context.getCatalogTable().getSchema().toPhysicalRowDataType();

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
                getValueDecodingFormat(helper);

        final int[] valueProjection = createValueFormatProjection(tableOptions, physicalDataType);

        return new DtsDynamicSource(server, topic, sid, user , password, checkpoint,
                physicalDataType, valueDecodingFormat, valueProjection);
    }

    private DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(FactoryUtil.TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(
                DeserializationFormatFactory.class, FactoryUtil.FORMAT).get();
    }
}
