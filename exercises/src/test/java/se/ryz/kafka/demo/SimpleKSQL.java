package se.ryz.kafka.demo;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import javax.json.Json;
import java.util.Properties;
import java.util.Random;

/*

 TOPIC_NAME=simple-ksql
 # Delete Topic if it exists
 kafka-topics --delete \
 --if-exists \
 --topic $TOPIC_NAME \
 --zookeeper localhost:2181,localhost:2182,localhost:2183

 # Create Topic
 PARTITION_COUNT=2
 REPLICATION_FACTOR=2

 kafka-topics --create \
 --topic $TOPIC_NAME \
 --partitions $PARTITION_COUNT \
 --replication-factor $REPLICATION_FACTOR \
 --if-not-exists \
 --config min.insync.replicas=2 \
 --zookeeper localhost:2181,localhost:2182,localhost:2183

  # Start ksql
  ksql

  # Create a stream of net events
  CREATE STREAM messages WITH (KAFKA_TOPIC='simple-ksql', VALUE_FORMAT='DELIMITED');
  SELECT * FROM messages;

  # Now run Run the produceMessages() method

  # In the KSQL shell:
  # We can view topics
  show topics;



  # When the produceMessages() method is running, we can print the messages
  print 'simple-ksql';
 */
public class SimpleKSQL {

    private void sendMessage(String topicName, Faker key, Faker value, KafkaProducer<String, String> producer) {
        String json = Json.createObjectBuilder()
                .add("character", key.lebowski().character())
                .add("plays", value.esports().game())
                .add("drinks", value.beer().name())
                .build()
                .toString();
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key.lebowski().actor(), json);
        producer.send(record);
        producer.flush();
        System.out.println("Sent " + record);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignore
        }

    }

    @Test
    public void produceMessages() throws InterruptedException {
        Common common = new Common();
        String topicName = "simple-ksql";

        Faker valueFaker = new Faker();
        Properties producerProperties = common.createProcessorProducerProperties("simpleKSQLProducer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        while (true) {
            Faker keyFaker = new Faker(new Random((int) (Math.random() * 5)));
            sendMessage(topicName, keyFaker, valueFaker, producer);
        }
    }
}
