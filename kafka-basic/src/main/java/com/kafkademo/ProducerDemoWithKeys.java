package com.kafkademo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        System.out.println("hello makhsodur");

        //create producer config
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        for(int i = 0; i<10; i++) {

            String topic = "demo-java";
            String value = "hello world " + i;
            String key = "id_" + i;
            // create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, value);

            //send the data
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null){
                        // the record was successfully sent
                        log.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Key: " + producerRecord.key() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("error is comming :" + e);
                    }
                }
            });
        }
        //flush
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
