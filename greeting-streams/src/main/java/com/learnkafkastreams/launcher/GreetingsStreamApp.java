package com.learnkafkastreams.launcher;

import com.learnkafkastreams.exceptionHandler.StreamProcessorCustomerErrorHandler;
import com.learnkafkastreams.exceptionHandler.StreamSerializationExceptionHandler;
import com.learnkafkastreams.exceptionHandler.StreamsDeserializationExceptionHandler;
import com.learnkafkastreams.topology.GreetingTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class GreetingsStreamApp {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
//        default error handler to continue after deserialization failure
//        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

        //custom deserialization exception handler
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamsDeserializationExceptionHandler.class);

        //custom serialization exception handler
        properties.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamSerializationExceptionHandler.class);


//        createTopics(properties, List.of(GreetingTopology.GREETINGS, GreetingTopology.GREETINGS_UPPERCASE));
        Topology topology = GreetingTopology.buildTopology();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        kafkaStreams.setUncaughtExceptionHandler(new StreamProcessorCustomerErrorHandler());
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        //executes topology
        try {
            //everytime you shut down application, the shutdown hook is going to close function of Kafka Streams
            kafkaStreams.start();
        }catch (Exception e) {
            log.error("Exception in starting the stream : {}", e.getMessage());
        }


    }

    private static void createTopics(Properties config, List<String> greetings) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 2;
        short replication = 1;

        var newTopics = greetings
                .stream()
                .map(topic -> {
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ", e.getMessage(), e);
        }
    }
}
