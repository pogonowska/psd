package org.example;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkConsumer {
    public static FlinkKafkaConsumer<InputMessage> createStringConsumerForTopic(
            String topic, String kafkaAddress, String kafkaGroup ) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id",kafkaGroup);
        FlinkKafkaConsumer<InputMessage> consumer = new FlinkKafkaConsumer<>(
                topic, new InputMessageDeserializationSchema(), props);

        return consumer;
    }
}
