package se.ryz.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.Future;

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


 */
public class SimpleKSQL {

    @Test
    public void produceMessages() throws InterruptedException {
        Common common = new Common();
        String topicName = "simple-ksql" ;
        Properties producerProperties = common.createProcessorProducerProperties("simpleKSQLProducer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (int cnt=0; ;cnt++) {
            System.out.println ("Producing message " + cnt + " to topic '" + topicName + "'");
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "Message no " + cnt, common.getNextLabel());
            producer.send(record);
            producer.flush();

            Thread.sleep(2000);
        }


    }
}
