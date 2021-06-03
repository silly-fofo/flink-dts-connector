package com.alibaba.flink.connectors.dts.datastream;

import com.alibaba.flink.connectors.dts.util.DtsUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.PropertyConfigurator;

import java.io.InputStream;
import java.util.Properties;

/** DtsExampleUtil. */
public class DtsExampleUtil {
    public static StreamExecutionEnvironment prepareExecutionEnv(ParameterTool parameterTool)
            throws Exception {

        initLog4j();

        if (parameterTool.getNumberOfParameters() < 3) {
            System.out.println(
                    "Missing parameters!\n"
                            + "Usage: Dts --broker-url <dts brokers> --topic <topic> "
                            + "--sid <subscribe sid> --user <user name> --password <password> "
                            + "--checkpoint <initial checkpoint>");
            throw new Exception(
                    "Missing parameters!\n"
                            + "Usage: Dts --broker-url <dts brokers> --topic <topic> "
                            + "--sid <subscribe sid> --user <user name> --password <password> "
                            + "--checkpoint <initial checkpoint>");
        }

//         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set conf
        Configuration conf = new Configuration();

        //specified a checkpoint to restore the program
//        conf.set(
//                SavepointConfigOptions.SAVEPOINT_PATH,
//                "file:///tmp/checkpoints/dts-checkpoint/b3572cdb686e6c7e2855400a3361850f/chk-225");

        conf.setLong("akka.ask.timeout", 10000);

        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 5000));

        // checkpoint
        env.enableCheckpointing(
                10000, CheckpointingMode.EXACTLY_ONCE); // create a checkpoint every 10 seconds
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints/dts-checkpoint"));

        env.getConfig()
                .setGlobalJobParameters(
                        parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // env.setParallelism(1);

        return env;
    }

    private static Properties initLog4j() {
        Properties properties = new Properties();
        InputStream log4jInput = null;
        try {
            log4jInput = Thread.currentThread().getContextClassLoader().getResourceAsStream("log4j.properties");
            PropertyConfigurator.configure(log4jInput);
        } catch (Exception e) {
        } finally {
            DtsUtil.swallowErrorClose(log4jInput);
        }
        return properties;
    }
}
