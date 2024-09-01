package com.learnkafkastreams.domain;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

@Slf4j
public record AlphabetWordAggregate(String key,
                                    Set<String> valueList,
                                    int runningCount) {


    public AlphabetWordAggregate() {
        this("", new HashSet<>(), 0);
    }

    public static void main(String[] args) {


        var al = new AlphabetWordAggregate();

    }

    public AlphabetWordAggregate updateNewEvents(String key, String newValue) {
        log.info("New Record. Key : {}, Value : {}", key, newValue);

        valueList.add(newValue);
        AlphabetWordAggregate aggregatedValue = new AlphabetWordAggregate(
                key,
                valueList,
                this.runningCount + 1);

        log.info("Aggregated value : {}", aggregatedValue);
        return aggregatedValue;
    }

}


