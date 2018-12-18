package se.ryz.kafka;


import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.Logger;
import se.ryz.kafka.avro.HelloWorldCommand;
import se.ryz.kafka.avro.HelloWorldRequest;

import java.net.URL;
import java.util.Properties;

public class HelloWorldService {

    public HelloWorldService(String kafkaBrokers, String schemaRegistry) {
        Properties producerConfig = new Properties();
        producerConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "hello-world-stream");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndIgnoreExceptionHandler.class.getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, HelloWorldCommand> helloWorldCommandConsumer = builder.stream("hello-world-command-topic");

        helloWorldCommandConsumer.foreach((messageId, command) -> {
            System.out.println("******************************* Received message. ID: " + messageId + ", command: " + command);
        });

        KafkaProducer<String, HelloWorldCommand> helloWorldCommandProducer = new KafkaProducer(producerConfig);
        String messageId = "1";
        HelloWorldRequest helloWorldRequest = new HelloWorldRequest(System.currentTimeMillis(), "The request");
        HelloWorldCommand helloWorldCommand = new HelloWorldCommand(System.currentTimeMillis(), helloWorldRequest);
        ProducerRecord<String, HelloWorldCommand> record = new ProducerRecord<>("hello-world-command-topic", messageId, helloWorldCommand);
        System.out.println ("******************************* Sending message");
        helloWorldCommandProducer.send(record);

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
for(;;) {
    try {
        Thread.sleep(1000);
    } catch (InterruptedException ex) {

    }
}
    }

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(Option.builder("k")
                .longOpt("kafka-brokers")
                .hasArg(true)
                .desc("The Kafka brokers. The broker address consists of 'host[:port]. If the optional port is not given, the default port 9092 is used")
                .required()
        .build());
        options.addOption(Option.builder("s")
                .longOpt("schema-registry")
                .hasArg(true)
                .desc("The host and optional port and protocol of the schema registry, ex: '[http[s]://}host[:port]. If the optional port is not given, the default port 8081 is used. If the optional protocol is not given, 'http' is used.")
                .required()
        .build());

        CommandLineParser parser = new BasicParser();
        CommandLine cmdLine = null;
        HelpFormatter hf = new HelpFormatter();

        try {
            cmdLine = parser.parse(options, args);
        } catch (MissingOptionException ex) {
            System.out.println(ex.getMessage());
            hf.printHelp(HelloWorldService.class.getSimpleName(), options);
            System.exit(1);
        }
        String kafkaBrokers = cmdLine.getOptionValue("k");
        String schemaRegistry = cmdLine.getOptionValue("s");
        new HelloWorldService(kafkaBrokers, schemaRegistry);
    }
}
