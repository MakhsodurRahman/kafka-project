package com.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ProducerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {

        System.out.println("hello makhsodur");

        String groupId = "my-group-2";
        String topic = "demo-java";
        //create producer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //none/earliest/latest


        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainTheard = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("detected a shutdown . call by consumer.wakeup()");
                consumer.wakeup();

                try{
                    mainTheard.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        try {
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key : " + record.key() + " value " + record.value());
                    log.info("partitions : " + record.partition() + " offset : " + record.offset());
                }
            }
        }catch (WakeupException e){
            log.info("wakeup exception");
        }catch (Exception e){
            log.error("Unexcepted exception");
        }finally {
            consumer.close();
            log.info("consumer close");
        }
    }
}
