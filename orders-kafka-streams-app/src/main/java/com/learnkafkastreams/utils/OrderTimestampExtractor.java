package com.learnkafkastreams.utils;

import com.learnkafkastreams.domain.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
public class OrderTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Order order = (Order)record.value();

        if(order != null && order.orderedDateTime() != null) {
            LocalDateTime timestamp = order.orderedDateTime();
            log.info("Extracted timestamp: {}", timestamp);

            return convertToInstant(timestamp);
        }


        return 0;
    }

    private long convertToInstant(LocalDateTime timestamp) {
        return timestamp.toInstant(ZoneOffset.ofHours(-2)).toEpochMilli();
    }
}
