package com.alibaba.flink.connectors.dts.sql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;

public class DtsTableIJoinTCase {

    protected  static StreamExecutionEnvironment env;
    protected static StreamTableEnvironment tEnv;

    public static void setup() {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        tEnv =  StreamTableEnvironment.create(env, settings);

        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);
    }

    public static void main(String[] args) throws Exception {
        setup();

        final String createKafkaTable =
                "create table `employee_action` (\n"
                        + "  `id` bigint,\n"
                        + "  `action` varchar,\n"
                        + "  `action_time` timestamp,\n"
                        + "  `employee_id` bigint\n"
                        + ") with (\n"
                        + "'connector' = 'dts',"
                        + "'dts.server' = 'xxx',"
                        + "'topic' = 'xxx',"
                        + "'dts.sid' = 'xxx', "
                        + "'dts.user' = 'xxx', "
                        + "'dts.password' = '***',"
                        + "'dts.checkpoint' = 'xxx', "
                        + "'dts-cdc.table.name' = 'yanmen_source.employee_action',"
                        + "'format' = 'dts-cdc')";

        final String createMysqlTable = "CREATE TABLE `employee` (\n"
                + "  `id` int,\n"
                + "  `name` varchar,\n"
                + "  `age` int\n"
                + ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://xxx:3306/yanmen_source',\n" +
                "   'username' = 'xxx'," +
                "   'password' = '***',\n" +
                "   'table-name' = '<table_name>',\n" +
                "   'driver' = 'com.mysql.jdbc.Driver',\n" +
                "   'lookup.cache.max-rows' = '500', \n" +
                "   'lookup.cache.ttl' = '10s',\n" +
                "   'lookup.max-retries' = '3'" +
                ")";

        String createJoinTable = "create view `employee_action_detail` as select "
                + "employee_action.id as id , \n"
                + "employee.name as employee_name, "
                + "employee.age as employee_age, "
                + "employee_action.action as action, "
                + "employee_action.action_time as action_time "
                + "from employee_action left join "
                + "employee on "
                + "employee_action.employee_id = employee.id "
                + "";

        tEnv.executeSql(createKafkaTable);
        tEnv.executeSql(createMysqlTable);
        tEnv.executeSql(createJoinTable);

        String query =
                "SELECT\n"
                        + "  `id`,\n"
                        + "  `employee_name`,\n"
                        + "  `employee_age`,\n"
                        + "  `action`,\n"
                        + "  `action_time`\n"
                        + "FROM employee_action_detail\n";

        tEnv.toRetractStream(tEnv.sqlQuery(query), RowData.class).print("######");

        env.execute();
    }
}
