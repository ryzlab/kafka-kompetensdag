package se.ryz.kafka.demo;

/*
 Create a Topic with two partitions

    TOPIC_NAME=kafka-log-topic

    # Delete Topic if it exists
    kafka-topics --delete \
    --if-exists \
    --topic $TOPIC_NAME \
    --zookeeper localhost:22181,localhost:32181,localhost:42181


    # Create Topic

    kafka-topics --create \
    --topic $TOPIC_NAME \
    --partitions 1 \
    --replication-factor 3 \
    --if-not-exists \
    --config min.insync.replicas=2 \
    --zookeeper localhost:22181,localhost:32181,localhost:42181

    # Run interactive shell
    docker exec -it kafka-1 bash

    # Check log file sizes, the log file should be empty
    ls -al /var/lib/kafka/data/kafka-log-topic-0

    # Produce messages to Topic with console producer

    kafka-console-producer \
    --broker-list localhost:29092,localhost:39092,localhost:49092 \
    --topic $TOPIC_NAME

    # Dump log
    docker exec -it kafka-1 bash -c "kafka-run-class kafka.tools.DumpLogSegments --deep-iteration --print-data-log --files /var/lib/kafka/data/kafka-log-topic-0/00000000000000000000.log"

    Open shell to Kafka-1 container and remove all data for topic
    docker exec -it kafka-1 bash
    rm -rf /var/lib/kafka/data/kafka-log-topic-0

    Produce a message with
     kafka-console-producer \
    --broker-list localhost:29092,localhost:39092,localhost:49092 \
    --topic $TOPIC_NAME

    No log file appears,
    ls -al /var/lib/kafka/data/kafka-log-topic-0
    ls: cannot access /var/lib/kafka/data/kafka-log-topic-0: No such file or directory

    Kill Kafka container
    docker kill kafka-1

    Open shell to Kafka-1 container and see that the data is there
    docker exec -it kafka-1 bash
    ls -al /var/lib/kafka/data/kafka-log-topic-0

    # And dump the log again
    docker exec -it kafka-1 bash -c "kafka-run-class kafka.tools.DumpLogSegments --deep-iteration --print-data-log --files /var/lib/kafka/data/kafka-log-topic-0/00000000000000000000.log"


 */
public class KafkaLog {
}
