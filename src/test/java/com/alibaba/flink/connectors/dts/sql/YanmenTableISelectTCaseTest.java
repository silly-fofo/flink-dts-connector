package com.alibaba.flink.connectors.dts.sql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

public class YanmenTableISelectTCaseTest {

    protected static StreamExecutionEnvironment env;
    protected static StreamTableEnvironment tEnv;

    public static void setup(String[] args) throws IOException {

        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String configFilePath = parameterTool.get("configFile");
        /*创建一个Properties对象，用于保存在平台中设置的相关参数值。*/
        //Properties properties = new Properties();

        /*将平台页面中设置的参数值加载到Properties对象中。*/
        //properties.load(new StringReader(new String(Files.readAllBytes(Paths.get(configFilePath)), StandardCharsets.UTF_8)));

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 5000));

        // checkpoint
        env.enableCheckpointing(
                10000, CheckpointingMode.EXACTLY_ONCE); // create a checkpoint every 10 seconds
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints/dts-checkpoint"));

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        tEnv = StreamTableEnvironment.create(env, settings);

        //env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);
    }

    public static void main(String[] args) throws Exception {
        setup(args);

        final String createTable =
                "create table `dts` (\n"
                        + "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',\n"
                        + "  `id` bigint,\n"
                        + "  `name` varchar,\n"
                        + "  `age` bigint,\n"
                        + " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND"
                        + ") with (\n"
                        + "'connector' = 'dts',"
                        + "'dts.server' = 'dts-cn-hangzhou.aliyuncs.com:18001',"
                        + "'topic' = 'cn_hangzhou_rm_bp11tv2923n87081s_rdsdt_dtsacct',"
                        + "'dts.sid' = 'dtshyye52491877bfk', "
                        + "'dts.user' = 'yanmen', "
                        + "'dts.password' = 'yanmen123',"
                        + "'dts.checkpoint' = '1622638753', "
                        + "'dts-cdc.table.name' = 'yanmen_source.test',"
                        + "'format' = 'dts-cdc')";

        tEnv.executeSql(createTable);

        String query =
                "SELECT\n"
                        + "  `ts` ,\n"
                        + "  `id`,\n"
                        + "  concat('dts-', `name`) as dtsname,\n"
                        + "  `age`\n"
                        + "FROM dts\n";

        tEnv.toRetractStream(tEnv.sqlQuery(query), RowData.class).print("######");

        env.execute();
    }
}
