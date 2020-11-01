package com.github.vamsha.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class producer_demo_keys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(producer_demo_keys.class);

        String bootstrapServers = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10; i++){
            String topic = "first_topic";
            String value = "Hello world"+Integer.toString(i);
            String key = "id_"+Integer.toString(i);
            //create record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("key: " + key);
            //id_0 -> 1
            //id_1 -> 0
            //id_2 -> 2
            //id_3 -> 0
            //id_4 -> 2
            //id_5 -> 2
            //id_6 -> 0
            //id_7 -> 2
            //id_8 -> 1
            //id_9 -> 2


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
            }).get(); //block the send to make it synchronous
        }

        //flush the record
        producer.flush();

        //flush and close the record
        producer.close();

    }
}
