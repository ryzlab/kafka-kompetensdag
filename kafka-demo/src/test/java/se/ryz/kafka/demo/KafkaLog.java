package se.ryz.kafka.demo;

/*
 Create a Topic with two partitions

    TOPIC_NAME=kafka-log-topic
    ZOOKEEPERS=172.18.0.10:22181,172.18.0.11:32181,172.18.0.12:42181

    # Delete Topic if it exists
    docker run \
        --net=confluentnet \
        --rm \
        confluentinc/cp-kafka:5.1.0 \
        kafka-topics --delete \
        --if-exists \
        --topic $TOPIC_NAME \
        --zookeeper $ZOOKEEPERS


    # Create Topic
    docker run \
        --net=confluentnet \
        --rm \
        confluentinc/cp-kafka:5.1.0 \
        kafka-topics --create \
        --topic $TOPIC_NAME \
        --partitions 1 \
        --replication-factor 3 \
        --if-not-exists \
        --config min.insync.replicas=2 \
        --zookeeper $ZOOKEEPERS

    # Run interactive shell
    docker exec -it kafka-1 bash
    cd /var/lib/kafka/data/kafka-log-topic-0

    # Check log file sizes, the log file should be empty
    ls -al

    # Produce messages to Topic with console producer
    docker run \
        -it \
        --net=confluentnet \
        --rm \
        confluentinc/cp-kafka:5.1.0 \
        kafka-console-producer \
        --broker-list 172.18.0.20:29092,172.18.0.21:39092,172.18.0.22:49092 \
        --topic $TOPIC_NAME

    or
    docker exec -it kafka-1 bash -c "kafka-console-producer --broker-list 172.18.0.20:29092,172.18.0.21:39092,172.18.0.22:49092 --topic $TOPIC_NAME"

    # Dump log
    docker exec -it kafka-1 bash -c "kafka-run-class kafka.tools.DumpLogSegments --deep-iteration --print-data-log --files /var/lib/kafka/data/kafka-log-topic-0/00000000000000000000.log"

    docker run \
        -it \
        --net=confluentnet \
        --rm \
        confluentinc/cp-kafka:5.1.0 \
        kafka-run-class kafka.tools.DumpLogSegments \
        --deep-iteration \
        --print-data-log \
        --files /var/lib/kafka/data/kafka-log-topic-0/00000000000000000000.log

    Open shell to Kafka-1 container and remove all data for topic
    docker exec -it kafka-1 bash
    rm -rf /var/lib/kafka/data/kafka-log-topic-0

    Produce a message with
    docker exec -it kafka-1 bash -c "kafka-console-producer --broker-list 172.18.0.20:29092,172.18.0.21:39092,172.18.0.22:49092 --topic $TOPIC_NAME"

    Kafka-1 crashes, restart
    docker start kafka-1

    Open shell to Kafka-1 container and see that the data is there
    docker exec -it kafka-1 bash
    ls -al /var/lib/kafka/data/kafka-log-topic-0






 */
public class KafkaLog {
}
