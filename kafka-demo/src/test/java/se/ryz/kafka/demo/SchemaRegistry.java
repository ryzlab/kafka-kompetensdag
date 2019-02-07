package se.ryz.kafka.demo;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;
import se.ryz.kafka.avro.HelloWorldCommand;
import se.ryz.kafka.avro.HelloWorldReply;
import se.ryz.kafka.avro.HelloWorldRequest;

import java.util.Properties;

/**

  # List subjects in Shcema Registry
  curl localhost:8081/subjects

 TOPIC_NAME=hello-world-avro

 # Delete Topic if it exists
 kafka-topics --delete \
 --if-exists \
 --topic $TOPIC_NAME \
 --zookeeper localhost:22181,localhost:32181,localhost:42181

 # Create Topic
 PARTITION_COUNT=2
 REPLICATION_FACTOR=2

 kafka-topics --create \
 --topic $TOPIC_NAME \
 --partitions $PARTITION_COUNT \
 --replication-factor $REPLICATION_FACTOR \
 --if-not-exists \
 --config min.insync.replicas=2 \
 --zookeeper localhost:22181,localhost:32181,localhost:42181



  Send an Avro Object to a topic and check what is registered
  run {@link SchemaRegistry#testSendWithDefaultNamingStrategy()}

  Try to send another Avro Object to the same topic and see what happens
 run {@link }

  Now, change the Value Subject Name Strategy to RecordNameStrategy, send the same avro object and check what is registered
  Try to send another Avro Object to the same topic and see what happens with the new naming strategy

 */
public class SchemaRegistry {

    @Test
    public void testConsumeWithDefaultNamingStrategy() throws InterruptedException{
        Common common = new Common();
        common.createConsumerConfig("consumer-" + this.getClass().getName(), "client-" + this.getClass().getName());
        Properties streamsConfiguration = common.createStreamsClientConfiguration(this.getClass().getName(), null);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());


        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, HelloWorldCommand> commandStream = builder.stream("hello-world-avro");
        commandStream.foreach((key, command) -> {
            System.out.println("Command received: " + command.getCommand());
            System.out.println ("Request? " + (command.getCommand() instanceof HelloWorldRequest));
            System.out.println ("Reply? " + (command.getCommand() instanceof HelloWorldReply));
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();
        //Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        for(;;) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void testSendWithDefaultNamingStrategy() {
        Common common = new Common();
        Properties props = common.createProcessorProducerProperties(null);
        // Override Value Serializer to be Avro
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Common.SCHEMA_REGISTRY_URL);
        // Default Key and Value name strategy is TopicNameStrategy

        KafkaProducer<String, HelloWorldCommand> producer = new KafkaProducer<>(props);
        HelloWorldRequest command = new HelloWorldRequest("The request");
        HelloWorldCommand helloWorldCommand = new HelloWorldCommand(command);
        ProducerRecord<String, HelloWorldCommand> record = new ProducerRecord<>("hello-world-avro", "key", helloWorldCommand);
        producer.send(record);
        producer.flush();
        producer.close();
    }

    @Test
    public void testSendWithRecordNameStrategy() {
        Common common = new Common();
        Properties props = common.createProcessorProducerProperties(null);
        // Override Value Serializer to be Avro
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        // Override the default Key and Value name strategy
        props.put(AbstractKafkaAvroSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        props.put(AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Common.SCHEMA_REGISTRY_URL);

        KafkaProducer<String, HelloWorldCommand> producer = new KafkaProducer<>(props);
        HelloWorldRequest command = new HelloWorldRequest("The request");
        HelloWorldCommand helloWorldCommand = new HelloWorldCommand(command);
        ProducerRecord<String, HelloWorldCommand> record = new ProducerRecord<>("hello-world-avro-recordnamestrategy", "key", helloWorldCommand);
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
