package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

@Slf4j
public class GreetingKTableTopology {
    public static String WORDS = "words";

    public static Topology build() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KTable<String, String> KTable = streamsBuilder
                .table(WORDS,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("words-store"))
                .filter((k, v) -> v.length() > 2)
                .mapValues(((readOnlyKey, value) -> value.toUpperCase()));

        KTable
                .toStream()
                .peek((k, v) -> log.info("Key : {}, Value: {}", k, v))
                .print(Printed.<String, String>toSysOut().withLabel("Words-KTable"));

        return streamsBuilder.build();
    }
}
