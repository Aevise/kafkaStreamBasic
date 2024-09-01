package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.domain.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
    public static final String RESTAURANT_ORDERS_REVENUE_WINDOWS = "restaurant_orders_revenue_windows";
    public static final String RESTAURANT_ORDERS_WINDOWS = "restaurant_orders_windows";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String GENERAL_ORDERS_REVENUE_WINDOWS = "general_orders_revenue_windows";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_WINDOWS = "general_orders_windows";
    public static final String STORES = "stores";

    private static final Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
    private static final Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

    private static final ValueMapper<Order, Revenue> revenueValueMapper = order -> new Revenue(order.locationId(), order.finalAmount());

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Order> ordersStream = streamsBuilder.stream(ORDERS,
                Consumed.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .selectKey((key, value) -> value.locationId());

        ordersStream
                .print(Printed.<String, Order>toSysOut().withLabel("Orders"));

        //KStream - KTable
        KTable<String, Store> storesTable = streamsBuilder
                .table(STORES,
                        Consumed.with(Serdes.String(), SerdesFactory.storeSerdes()));

        ordersStream
                .split(Named.as("General-restaurant-stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(
                                generalOrdersStream -> {
                                    generalOrdersStream
                                            .print(Printed.<String, Order>toSysOut().withLabel("General Stream"));

//                                    generalOrdersStream
//                                            .mapValues((readOnlyKey, value) -> revenueValueMapper.apply(value))
//                                            .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));

//                                    aggregateOrdersByCount(generalOrdersStream, GENERAL_ORDERS_COUNT);
//                                    aggregateOrdersByRevenue(generalOrdersStream, GENERAL_ORDERS_REVENUE, storesTable);
//                                    aggregateOrdersCountByTimeWindows(generalOrdersStream, GENERAL_ORDERS_WINDOWS, storesTable);
                                    aggregateOrdersByRevenueWindows(generalOrdersStream, GENERAL_ORDERS_REVENUE_WINDOWS, storesTable);
                                }
                        ))
                .branch(restaurantPredicate,
                        Branched.withConsumer(
                                restaurantOrdersStream -> {
                                    restaurantOrdersStream
                                            .print(Printed.<String, Order>toSysOut().withLabel("Restaurant Stream"));

//                                    restaurantOrdersStream
//                                            .mapValues((readOnlyKey, value) -> revenueValueMapper.apply(value))
//                                            .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));

//                                    aggregateOrdersByCount(restaurantOrdersStream, RESTAURANT_ORDERS_COUNT);
//                                    aggregateOrdersByRevenue(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE, storesTable);
//                                    aggregateOrdersCountByTimeWindows(restaurantOrdersStream, RESTAURANT_ORDERS_WINDOWS, storesTable);
                                    aggregateOrdersByRevenueWindows(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE_WINDOWS, storesTable);
                                }
                        ));

        return streamsBuilder.build();

    }

    private static void aggregateOrdersByRevenueWindows(KStream<String, Order> ordersStream, String storeName, KTable<String, Store> storesTable) {
        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;
        Duration duration = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(duration);

        Aggregator<String, Order, TotalRevenue> totalRevenueAggregator = (key, value, aggregate) -> aggregate.updateRevenue(key, value);

        KTable<Windowed<String>, TotalRevenue> revenueTable = ordersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .windowedBy(timeWindows)
                .aggregate(
                        totalRevenueInitializer,
                        totalRevenueAggregator,
                        Materialized.<String, TotalRevenue, WindowStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerde())
                );

        revenueTable
                .toStream()
                .peek((key, value) -> {
                    log.info("Windowed Revenue. Store name : {}, key : {}, value : {}", storeName, key, value);

                })
                .print(Printed.<Windowed<String>, TotalRevenue>toSysOut().withLabel(storeName + " Windowed Revenue"));

        //KTable - KTable join
        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        Joined<String, TotalRevenue, Store> joinedParams = Joined.with(Serdes.String(), SerdesFactory.totalRevenueSerde(), SerdesFactory.storeSerdes());

        revenueTable
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .join(storesTable, valueJoiner, joinedParams)
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storeName + "-byStore"));

    }

    private static void aggregateOrdersCountByTimeWindows(KStream<String, Order> ordersStream, String storeName, KTable<String, Store> storesTable) {
        Duration duration = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(duration);

        KTable<Windowed<String>, Long> orderCountByStore = ordersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .windowedBy(timeWindows)
                .count(Named.as(storeName), Materialized.as(storeName))
                .suppress(
                        Suppressed
                                .untilWindowCloses(Suppressed.BufferConfig.unbounded()
                                        .shutDownWhenFull())
                );

        orderCountByStore
                .toStream()
                .peek((key, value) -> {
                    log.info("Store name : {}, key : {}, value : {}", storeName, key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(storeName + " Time Windowed"));

//        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;
//
//        KTable<Windowed<String>, TotalCountWithAddress> revenueWithStoreTable = orderCountByStore.join(storesTable, valueJoiner);
//        revenueWithStoreTable
//                .toStream()
//                .print(Printed.<Windowed<String>, TotalCountWithAddress>toSysOut().withLabel(storeName+"-bytestore"));
    }

    private static void aggregateOrdersByRevenue(KStream<String, Order> ordersStream, String ordersRevenue, KTable<String, Store> storesTable) {
        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;
        Aggregator<String, Order, TotalRevenue> totalRevenueAggregator = (key, value, aggregate) -> aggregate.updateRevenue(key, value);

        KTable<String, TotalRevenue> revenueTable = ordersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .aggregate(
                        totalRevenueInitializer,
                        totalRevenueAggregator,
                        Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(ordersRevenue)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerde())
                );
        revenueTable
                .toStream()
                .print(Printed.<String, TotalRevenue>toSysOut().withLabel(ordersRevenue));

        //KTable - KTable join
        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        KTable<String, TotalRevenueWithAddress> revenueWithStoreTable = revenueTable
                .join(storesTable, valueJoiner);

        revenueWithStoreTable
                .toStream()
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(ordersRevenue + "-byStore"));


    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrdersStream, String storeName) {

        KTable<String, Long> orderCountByStore = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .count(Named.as(storeName), Materialized.as(storeName));

        orderCountByStore
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(storeName));
    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrdersStream, String storeName, KTable<String, Store> storesTable) {

        KTable<String, Long> ordersCountPerStore = generalOrdersStream
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .count(Named.as(storeName), Materialized.as(storeName));

        ordersCountPerStore
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(storeName));


        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

        KTable<String, TotalCountWithAddress> revenueWithStoreTable = ordersCountPerStore
                .join(storesTable, valueJoiner);

        revenueWithStoreTable
                .toStream()
                .print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(storeName+"-bystore"));
    }
}
