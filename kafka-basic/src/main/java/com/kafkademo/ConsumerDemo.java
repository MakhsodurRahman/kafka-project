package com.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        System.out.println("hello makhsodur");

        String groupId = "my-group";
        String topic = "mytopic";
        //create producer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //none/earliest/latest


        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        consumer.subscribe(Arrays.asList(topic));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record: records){
                log.info("key : "+record.key()+" value "+record.value());
                log.info("partitions : "+record.partition()+ " offset : "+record.offset());
            }
        }



    }
}
