package com.learnkafkastreams.topology;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
public class ExploreWindowTopology {

    public static final String WINDOW_WORDS = "windows-words";

    public static Topology build() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> wordsStream = streamsBuilder
                .stream(WINDOW_WORDS,
                        Consumed.with(Serdes.String(), Serdes.String()));

//        tumblingWindows(wordsStream);
        hoppingWindows(wordsStream);
        return streamsBuilder.build();
    }

    private static void tumblingWindows(KStream<String, String> wordsStream) {
        Duration windowSize = Duration.ofSeconds(5);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

        KTable<Windowed<String>, Long> windowedTable = wordsStream
                .groupByKey()
                .windowedBy(timeWindows)
                .count()
                .suppress(
                        Suppressed
                                .untilWindowCloses(Suppressed.BufferConfig.unbounded()
                                        .shutDownWhenFull())
                );

        windowedTable
                .toStream()
                .peek((key, value) -> {
                    log.info("For key {} count is {}", key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(WINDOW_WORDS));
    }

    private static void hoppingWindows(KStream<String, String> wordsStream) {
        Duration windowSize = Duration.ofSeconds(5);
        Duration advanceBy = Duration.ofSeconds(3);
        TimeWindows timeWindows = TimeWindows
                .ofSizeWithNoGrace(windowSize)
                .advanceBy(advanceBy);

        KTable<Windowed<String>, Long> windowedTable = wordsStream
                .groupByKey()
                .windowedBy(timeWindows)
                .count()
                .suppress(
                        Suppressed
                                .untilWindowCloses(Suppressed.BufferConfig.unbounded()
                                        .shutDownWhenFull())
                );

        windowedTable
                .toStream()
                .peek((key, value) -> {
                    log.info("For key {} count is {}", key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(WINDOW_WORDS));
    }
    private static void slidingWindows(KStream<String, String> wordsStream) {
        Duration windowSize = Duration.ofSeconds(5);
        SlidingWindows timeWindows = SlidingWindows
                .ofTimeDifferenceWithNoGrace(windowSize);

        KTable<Windowed<String>, Long> windowedTable = wordsStream
                .groupByKey()
                .windowedBy(timeWindows)
                .count()
                .suppress(
                        Suppressed
                                .untilWindowCloses(Suppressed.BufferConfig.unbounded()
                                        .shutDownWhenFull())
                );

        windowedTable
                .toStream()
                .peek((key, value) -> {
                    log.info("For key {} count is {}", key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(WINDOW_WORDS));
    }


    private static void printLocalDateTimes(Windowed<String> key, Long value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();

        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }

}
