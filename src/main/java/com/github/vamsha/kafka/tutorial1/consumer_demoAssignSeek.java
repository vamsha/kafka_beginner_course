package com.github.vamsha.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class consumer_demoAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(consumer_demoAssignSeek.class);

        //create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //assign and seek are mostly used to replay the data or fetch specific messages
        //assign
        TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        long offsetToReadFrom = 15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessageToRead=5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        //poll consumer
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                numberOfMessagesReadSoFar += 1;
                logger.info("Key : " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset" + record.offset());
                if (numberOfMessagesReadSoFar >= numberOfMessageToRead) {
                    keepOnReading = false; // exit while loop
                    break;  // exit for loop
                }
            }
        }
        logger.info("Exiting the Application ");
    }
}
