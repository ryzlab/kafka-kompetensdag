package se.ryz.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class DemoTest {

    private static final String KAFKA_DOCKER_HOST = "192.168.99.100";

    public static final String KAFKA_BROKERS =
            KAFKA_DOCKER_HOST + ":29092" /*+
            KAFKA_DOCKER_HOST + ":39092," +
                    KAFKA_DOCKER_HOST + ":49092"*/;

    private Properties createStreamsConfig() {

        final String bootstrapServers = KAFKA_BROKERS;
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcountqq"+System.currentTimeMillis());
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcountqq"+System.currentTimeMillis());
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    private Properties createProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    /**
     * Broker default config is to auto-create topics with
     * a replication factor, partition count and min-insync replicas of 1
     */
    @Test
    public void testAutoCreateProducer() throws InterruptedException {
        Properties producerProperties = createProducerProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for(int cnt=0;;cnt++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("everythingtopic", "msgkey", "msgvalue");
            producer.send(record);

            System.out.println("Sent record " + cnt);
            Thread.sleep(2000);
        }
    }

    @Test
    public void testAutoCreateProcessor() throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put("group.id", "dynamic-" + System.currentTimeMillis());
        //props.put("consumer.id", "autocreateconsumer3" + System.currentTimeMillis());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("everythingtopic"));

        for(;;) {
            System.out.println("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofHours(2));
            System.out.println("Received records: " + records.isEmpty() + ", num: " + records.count());
            //if (!records.isEmpty()) {
                for (ConsumerRecord record : records) {
                    System.out.println("Key: " + record.key() + ", message: " + record.value());
                }
            //}
        }
    }

    public static void main(String[] args)  throws InterruptedException{
        new DemoTest();
    }

    public DemoTest() throws InterruptedException {
        Properties streamsConfiguration = createStreamsConfig();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream("everything-topic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        textLines.foreach((key, value) -> System.out.println("Key: " + key + ", value: " + value));
streams.cleanUp();
        streams.start();

    }

    @Test
    public void runEverythingTopicStreamConsumer() throws InterruptedException {
        Properties streamsConfiguration = createStreamsConfig();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(Arrays.asList("everythingtopic"));
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        textLines.foreach((key, value) -> System.out.println("Key: "));

        streams.start();

        //Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        for (;;) {
            Thread.sleep(1000);
        }

    }

    @Test
    public void testSimpleConsumer() throws InterruptedException {

        Properties streamsConfiguration = createStreamsConfig();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // In the subsequent lines we define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        // Construct a `KStream` from the input topic "streams-plaintext-input", where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).
        //
        // Note: We could also just call `builder.stream("streams-plaintext-input")` if we wanted to leverage
        // the default serdes specified in the Streams configuration above, because these defaults
        // match what's in the actual topic.  However we explicitly set the deserializers in the
        // call to `stream()` below in order to show how that's done, too.
        final KStream<String, String> textLines = builder.stream("everythingtopic");
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

        textLines.foreach((key, value) -> System.out.println("Key: " + key + ", value: " + value));

        streams.start();
        for (;;) {
            Thread.sleep(1000);
        }
        //Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    @Test
    public void viewTopicConfig() {
        final String bootstrapServers = KAFKA_BROKERS;
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-lambda-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // Set up serializers and deserializers, which we will use for overriding the default serdes
        // specified above.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        // In the subsequent lines we define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        // Construct a `KStream` from the input topic "streams-plaintext-input", where message values
        // represent lines of text (for the sake of this example, we ignore whatever may be stored
        // in the message keys).
        //
        // Note: We could also just call `builder.stream("streams-plaintext-input")` if we wanted to leverage
        // the default serdes specified in the Streams configuration above, because these defaults
        // match what's in the actual topic.  However we explicitly set the deserializers in the
        // call to `stream()` below in order to show how that's done, too.
        final KStream<String, String> textLines = builder.stream("streams-plaintext-input");
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
