package com.alibaba.flink.connectors.dts.fetcher;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

/** Util class for kafka. */
public class DtsKafkaUtil {

    public static Properties getKafkaProperties(
            String brokerUrl,
            String topic,
            String sid,
            String group,
            String user,
            String password,
            Properties kafkaExtraProps) {
        Properties props = new Properties();

        if (StringUtils.isNotEmpty(user) && StringUtils.isNotEmpty(password)) {
            props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, buildJaasConfig(sid, user, password));
            props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");
        }

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, StringUtils.isNotEmpty(group) ? group : sid);
        // disable auto commit
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        props.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        // to let the consumer feel the switch of cluster and reseek the offset by timestamp
        props.setProperty(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ClusterSwitchListener.class.getName());

        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        if (kafkaExtraProps != null) {
            props.putAll(kafkaExtraProps);
        }

        return props;
    }

    public static String buildJaasConfig(String sid, String user, String password) {

        if (StringUtils.isNotEmpty(sid)) {
            String jaasTemplate =
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s-%s\" password=\"%s\";";
            return String.format(jaasTemplate, user, sid, password);
        } else {
            String jaasTemplate =
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
            return String.format(jaasTemplate, user, password);
        }
    }
}
