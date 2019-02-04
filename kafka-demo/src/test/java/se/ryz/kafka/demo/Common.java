package se.ryz.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Common {
    private static final String KAFKA_DOCKER_HOST = "192.168.99.100";

    public static final String KAFKA_BROKERS =
            "172.18.0.20:29092," +
                    "172.18.0.21:39092," +
                    "172.18.0.22:49092";

    public static final String SCHEMA_REGISTRY_URL = "172.18.0.40:8081";


    public Properties createConsumerConfig(String groupId, String clientId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // CLIENT_ID_CONFIG is used so that we can track calls from the client in the server logs
        if (clientId != null) {
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        // Set MAX_POLL_INTERVAL_MS_CONFIG when poll will block for a long time. Otherwise the consumer
        // will think that the broker is down
        //props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    /**
     * sets up a Processor producer
     *      Processor Client ID
     *      Kafka Bootstrap servers
     *      Key and Value SerDes (both as String)
     * @param clientId
     * @return
     */
    public Properties createProcessorProducerProperties(String clientId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        // Used to trace activity in Kafka logs
        if (clientId != null) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        }
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    /**
     * Run the producer once to produce records with null key.
     * This will use round-robin scheme when producing messages to partitions. Each partition
     * will get every other message
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void runProducerWithNullKey(String topicName, String clientId) throws InterruptedException, ExecutionException {
        Common common = new Common();
        Properties producerProperties = common.createProcessorProducerProperties(clientId);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (int cnt=0; ;cnt++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, Integer.toString(cnt));
            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            producer.flush();
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.println(recordMetadata.topic() +": key=null, value=" + cnt + ", offset: " + recordMetadata.offset() + ", partition: " + recordMetadata.partition());
            Thread.sleep(2000);
        }
        //producer.close();
    }

    public void runSubscriptionConsumer(String topicName, String groupId, String clientId) throws InterruptedException {
        Common common = new Common();
        Properties consumerConfig = common.createConsumerConfig(groupId, clientId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Arrays.asList(topicName));

        for (;;) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.isEmpty()) {
                System.out.println("No records received") ;
            } else {
                for (Iterator<ConsumerRecord<String, String>> it = consumerRecords.iterator(); it.hasNext();) {
                    ConsumerRecord<String, String> record = it.next();
                    System.out.println ("   " + record.topic() + ", partition: " + record.partition() + ", key: " + record.key() + ", value: " + record.value());
                }
            }

        }
    }

    public Properties createStreamsClientConfiguration(String applicationId, String clientId) {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run. A new Application ID -> consume from beginning
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);

        // Client ID:
        // An optional identifier of a Kafka consumer (in a consumer group) that is passed to a Kafka broker with every request.
        // The sole purpose of this is to be able to track the source of requests beyond just ip and port by allowing a logical application name to be included in Kafka logs and monitoring aggregates.
        if (clientId != null) {
            streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
        }

        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);

        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        // If we do not commit and restart, we will get messages since last commit again when we restart
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "5000");
        return streamsConfiguration;
    }


}
