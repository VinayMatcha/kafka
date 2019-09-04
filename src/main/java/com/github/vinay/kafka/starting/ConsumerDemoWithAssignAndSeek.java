package com.github.vinay.kafka.starting;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithAssignAndSeek {


    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String boostrapServers = "127.0.0.1:9092";
        String groupId = "application-assign-seek";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create cinsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);


        //assign partiotn to read from
        TopicPartition topicPartitionToReadFrom = new TopicPartition(topic, 1);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(topicPartitionToReadFrom));

        //seek from parttion
        consumer.seek(topicPartitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean continueReading = true;
        int numberOfMessagesReadSoFar = 0;

        //poll for new data
        while(continueReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", offset: " + record.offset());
                numberOfMessagesReadSoFar++;
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    continueReading = false;
                    break;
                }
            }

        }
        logger.info("Exiting the consumer application");

    }


}
