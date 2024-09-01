package com.learnkafkastreams.domain;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;

@Slf4j
public record TotalRevenue(String locationId,
                           Integer runningOrderCount,
                           BigDecimal runningRevenue) {
    public TotalRevenue() {
        this("", 0, BigDecimal.ZERO);
    }

    public TotalRevenue updateRevenue(String key, Order order) {
        Integer newOrdersCount = this.runningOrderCount + 1;
        BigDecimal newRevenue = this.runningRevenue.add(order.finalAmount());

        log.info("Order count : {}, Total revenue : {}", newOrdersCount, newRevenue);

        return new TotalRevenue(key, newOrdersCount, newRevenue);

    }
}
