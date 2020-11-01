package com.github.vamsha.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producer_demo_withcallback {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(producer_demo_withcallback.class);

        String bootstrapServers = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10; i++){
            //create record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World" + Integer.toString(i));

            //send record
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing the record", e);
                    }
                }
            });
        }

        //flush the record
        producer.flush();

        //flush and close the record
        producer.close();

    }
}
