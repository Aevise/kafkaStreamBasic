package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Alphabet;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

@Slf4j
public class ExploreJoinsOperatorsTopology {


    public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    public static String ALPHABETS_ABBREVATIONS = "alphabets_abbreviations"; // A=> Apple


    public static Topology build() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

//        joinKStreamWithKTable(streamsBuilder);
//joinKStreamWithGlobalKTable(streamsBuilder);
        joinKTableWithKTable(streamsBuilder);
        return streamsBuilder.build();
    }

    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder) {
        KStream<String, String> alphabetAbbreviations = streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS,
                        Consumed.with(Serdes.String(), Serdes.String()));

        alphabetAbbreviations
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

        KTable<String, String> alphabetsTable = streamsBuilder
                .table(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabets-store"));

        alphabetsTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        KStream<String, Alphabet> joinedStream = alphabetAbbreviations
                .join(alphabetsTable, valueJoiner);
        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabets-abbreviations"));
    }

    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder) {
        KStream<String, String> alphabetAbbreviations = streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS,
                        Consumed.with(Serdes.String(), Serdes.String()));

        alphabetAbbreviations
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

        GlobalKTable<String, String> alphabetsTable = streamsBuilder
                .globalTable(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabets-store"));


        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        KeyValueMapper<String, String, String> keyValueMapper = (leftKey, rightKey) -> leftKey;

        KStream<String, Alphabet> joinedStream = alphabetAbbreviations
                .join(alphabetsTable, keyValueMapper, valueJoiner);


        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabets-abbreviations"));
    }

    private static void joinKTableWithKTable(StreamsBuilder streamsBuilder) {
        KTable<String, String> alphabetAbbreviations = streamsBuilder
                .table(ALPHABETS_ABBREVATIONS,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabets-abbreviations-store"));

        alphabetAbbreviations
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

        KTable<String, String> alphabetsTable = streamsBuilder
                .table(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabets-store"));

        alphabetsTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        KTable<String, Alphabet> joinedStream = alphabetAbbreviations
                .join(alphabetsTable, valueJoiner);
        joinedStream
                .toStream()
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabets-abbreviations"));
    }

    private static void joinKStreamWithKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> alphabetAbbreviations = streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS,
                        Consumed.with(Serdes.String(), Serdes.String()));

        alphabetAbbreviations
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS_ABBREVATIONS));

        KStream<String, String> alphabetsStream = streamsBuilder
                .stream(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsStream
                .print(Printed.<String, String>toSysOut().withLabel(ALPHABETS));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;
        JoinWindows fiveSecondsWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));
        StreamJoined<String, String, String> joinedParams = StreamJoined
                .with(Serdes.String(), Serdes.String(), Serdes.String());

        KStream<String, Alphabet> joinedStream = alphabetAbbreviations
                .join(alphabetsStream,
                        valueJoiner,
                        fiveSecondsWindow,
                        joinedParams
                );

        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabets-abbreviations-KStreamWithKStream"));
    }
}
