package com.xxx.rpc.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by MK33 on 2016/12/28.
 */
public class KafkaConsumerTest {

    public static void main(String args[]) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.201.129.75:9092,10.201.129.80:9092,10.201.129.81:9092");
//        props.put("zookeeper.connect", "10.201.129.75:2182");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
//        kafkaConsumer.subscribe(Arrays.asList("my-topic"));
//        while (true) {
//            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
//            for (ConsumerRecord<String, String> record : records)
//                System.out.printf("topic = %s, offset = %d, key = %s, value = %s", record.topic(), record.offset(), record.key(), record.value());
//        }
    }

}
