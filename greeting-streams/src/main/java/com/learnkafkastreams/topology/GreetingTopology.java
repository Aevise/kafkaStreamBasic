package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.domain.serdes.GreetingSerdes;
import com.learnkafkastreams.domain.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class GreetingTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

//        KStream<String, String> modifiedStream = getGreetingKStream(streamsBuilder);

        var modifiedStream = getCustomGreetingKStream(streamsBuilder);


//        modifiedStream
//                .mapValues((key, value) -> new Greeting(value.message().toUpperCase(), value.timeStamp()))
////                .print(Printed.<String, String>toSysOut().withLabel("Modified Stream"));
//        .print(Printed.<String, Greeting>toSysOut().withLabel("Modified Stream"));



        KStream<String, Greeting> exploreErrors = exploreErrors(modifiedStream);

        //providing output to external topic
        exploreErrors
                .to(GREETINGS_UPPERCASE
//                        , Produced.with(Serdes.String(), Serdes.String())
                        , Produced.with(Serdes.String(), SerdesFactory.greetingSerdesForGenerics())
                );

       return streamsBuilder.build();
    }

    private static KStream<String, Greeting> exploreErrors(KStream<String, Greeting> stream) {
        return stream
                .mapValues((key, value) -> {
                    if(value.message().equals("Transient Error")){
                        try{
                            throw new IllegalStateException(value.message());
                        }catch (Exception e){
                            log.error("Exception in exploreErrors : {}", e.getMessage());
                            return null;
                        }
                    }
                    return new Greeting(value.message().toUpperCase(), value.timeStamp());
                })
                .filter((key, value) -> key != null && value != null);
    }

    private static KStream<String, String> getGreetingKStream(StreamsBuilder streamsBuilder) {
        //read messages from Kafka topic
        KStream<String, String> greetingsStream = streamsBuilder
                .stream(GREETINGS
//                        ,Consumed.with(Serdes.String(), Serdes.String())
                );
        greetingsStream
                .print(Printed.<String, String>toSysOut().withLabel("Greetings Stream"));

        //modify stream
        return greetingsStream
                .filter(((key, value) -> value.length() > 5))
                .mapValues((readOnlyKey, value) -> value.toUpperCase());
    }

    private static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder streamsBuilder) {
        //read messages from Kafka topic
        KStream<String, Greeting> greetingsStream = streamsBuilder
                .stream(GREETINGS
                        ,Consumed.with(Serdes.String(), SerdesFactory.greetingSerdesForGenerics())
                );
        greetingsStream
                .print(Printed.<String, Greeting>toSysOut().withLabel("Greetings Stream"));

        //modify stream
        return greetingsStream;
    }
}
