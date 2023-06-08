package org.example;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class FlinkProducer {
    public static FlinkKafkaProducer<OutputMessage> createStringProducer(
            String topic, String kafkaAddress){

        return new FlinkKafkaProducer<OutputMessage>(kafkaAddress,
                topic, new OutputMessageSerializationSchema());
    }

}
