package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class GreetingTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //read messages from Kafka topic
        KStream<String, String> greetingsStream = streamsBuilder
                .stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));
        greetingsStream
                .print(Printed.<String, String>toSysOut().withLabel("Greetings Stream"));

        //modify stream
        KStream<String, String> modifiedStream = greetingsStream
                .mapValues((readOnlyKey, value) -> value.toUpperCase());
        modifiedStream
                .print(Printed.<String, String>toSysOut().withLabel("Modified Stream"));


        //providing output to external topic
        modifiedStream
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

       return streamsBuilder.build();
    }
}
