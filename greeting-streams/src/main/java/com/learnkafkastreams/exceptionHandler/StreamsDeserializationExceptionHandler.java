package com.learnkafkastreams.exceptionHandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

@Slf4j
public class StreamsDeserializationExceptionHandler implements DeserializationExceptionHandler {

    private final static int maxErrorCount = 2;
    private static int errorCounter = 0;

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        log.error("Exception is : {}, and the Kafka record is : {}", exception.getMessage(), record, exception);
        if (errorCounter < maxErrorCount) {
            errorCounter++;
            log.info("Errors left: {}", maxErrorCount - errorCounter);
            return DeserializationHandlerResponse.CONTINUE;
        }
        log.error("Maximum error count reached : {}. Shutting down application", maxErrorCount);
        return DeserializationHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
