package se.ryz.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import se.ryz.kafka.demo.util.Common;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class PollTimeout {

    @Test
    public void pollLongSleep() throws InterruptedException {
        String topicName = "slow-poll-topic";
        String clientId = "slowClient";
        String groupId = "slowGroup";
        Common common = new Common();
        Properties props = common.createConsumerConfig(groupId, clientId);
        // Create the consumer and subscribe
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        // Poll for records and process them
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
        System.out.println("Sleeping forever");
        Thread.sleep(3600000*24);
    }
}
