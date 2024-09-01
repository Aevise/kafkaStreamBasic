package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.AlphabetWordAggregate;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ExploreAggregateOperatorsTopology {


    public static String AGGREGATE = "aggregate";

    public static Topology build() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputStream = streamsBuilder
                .stream(AGGREGATE, Consumed.with(Serdes.String(), Serdes.String()));

        inputStream
                .print(Printed.<String, String>toSysOut().withLabel(AGGREGATE));


//        exploreCount(inputStream);
//        exploreReduce(inputStream);
        exploreAggregate(inputStream);

        return streamsBuilder.build();
    }

    private static void exploreAggregate(KStream<String, String> inputStream) {
        KGroupedStream<String, String> groupedStream = groupInput(inputStream);

        Initializer<AlphabetWordAggregate> alphabetWordAggregateInitializer = AlphabetWordAggregate::new;

        Aggregator<String, String, AlphabetWordAggregate> alphabetWordAggregateAggregator =
                (key, value, aggregate) -> aggregate.updateNewEvents(key, value);


        KTable<String, AlphabetWordAggregate> aggregatedStream = groupedStream
                .aggregate(alphabetWordAggregateInitializer,
                        alphabetWordAggregateAggregator,
                        Materialized.<String, AlphabetWordAggregate, KeyValueStore<Bytes, byte[]>>as("agggregated-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.alphabetWordAggregate())
                );

        aggregatedStream
                .toStream()
                .print(Printed.<String, AlphabetWordAggregate>toSysOut().withLabel("Aggregated Stream"));
    }

    private static void exploreReduce(KStream<String, String> inputStream) {
        KGroupedStream<String, String> groupedStream = groupInput(inputStream);

        KTable<String, String> reducedStream = groupedStream
                .reduce((value1, value2) -> {
                            log.info("Value1 : {}, Value 2: {}", value1, value2);
                            return (value1 + "-" + value2).toUpperCase();
                        },
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduced-words")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                );

        reducedStream
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("reduced-words"));
    }

    private static void exploreCount(KStream<String, String> inputStream) {
        KGroupedStream<String, String> groupedStream = groupInput(inputStream);

        KTable<String, Long> countAsAlphabet = groupedStream
                .count(Named.as("count-per-alphabet"),
                        Materialized.as("count-per-alphabet"));

        countAsAlphabet
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel("with-count-per-alphabet"));


    }

    private static KGroupedStream<String, String> groupInput(KStream<String, String> inputStream) {
        return inputStream
                .groupBy((key, value) -> key, Grouped.with(Serdes.String(), Serdes.String()));
//                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
    }

}
