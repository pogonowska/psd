package org.example;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OutputMessageSerializationSchema
        implements SerializationSchema<OutputMessage> {

    ObjectMapper objectMapper;
    Logger logger = LoggerFactory.getLogger(OutputMessageSerializationSchema.class);

    @Override
    public byte[] serialize(OutputMessage message) {
        if(objectMapper == null) {
            objectMapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule());
        }
        try {
            return objectMapper.writeValueAsString(message).getBytes();
        } catch (Exception e) {
            logger.error("Failed to parse JSON", e);
        }
        return new byte[0];
    }
}
