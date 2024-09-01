package com.learnkafkastreams.domain.serdes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
@AllArgsConstructor
public class JsonDeserializer <T> implements Deserializer<T> {

    private Class<T> destinationClass;

    private final ObjectMapper objectMapper =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);


    @Override
    public T deserialize(String topic, byte[] data) {
        try{
        return objectMapper.readValue(data, destinationClass);
    } catch (
    IOException e) {
        log.error("IOException in deserializer : {}", e.getMessage(), e);
        throw new RuntimeException(e);
    } catch (Exception e) {
        log.error("Exception in deserializer : {}", e.getMessage(), e);
        throw new RuntimeException(e);
    }
    }
}
