package com.xxx.rpc.test;

import org.apache.kafka.clients.producer.*;
import org.json.JSONObject;

import java.io.*;
import java.util.Properties;

/**
 * KafkaProducer 特点：
 * 线程安全，多个线程之间共享一个实例比每个线程一个实例效率高
 * <href>http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html</>
 * Created by MK33 on 2016/12/27.
 */
public class KafkaProduct {

    public static void main(String[] args) throws InterruptedException, IOException {
        Properties props = new Properties();
        props.load(new InputStreamReader(new FileInputStream("kafka.properties")));
//        props.put("bootstrap.servers", "10.201.129.75:9092,10.201.129.80:9092,10.201.129.81:9092");
//        props.put("acks", "all");
//        props.put("retries", 0); // 失败重试次数，0 代表不重试，大于0可能会导致数据重复
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1); //每隔1ms发送一次数据
//        props.put("buffer.memory", 33554432); // 生产者总的缓存大小，超出则会阻塞send()请求，为防止block可以设置block.on.buffer.full=false
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        String s = "{\"fireFirstRegEventRequired\":false,\"fireBindCard\":false,\"fireShangHaiBankPromotion\":false,\"firstRealName\":false,\"memberId\":100000000006120,\"loginCode\":\"34185691128\",\"mobile\":\"13761804422\",\"email\":\"paazm3813@bl.com\",\"loginPasswd\":\"b605e86d02eef8bfd0646f6a704c17c9\",\"custType\":\"01\",\"memberName\":\"张莹华\",\"birthYear\":0,\"birthMonth\":5,\"birthDay\":20,\"birtyLunarMonth\":0,\"birthLunarDay\":0,\"certCode\":\"311112189001014444\",\"companyName\":\"百联\",\"companyAddress\":\"上海\",\"registerTime\":\"Apr 16, 2015 4:10:36 PM\",\"channelId\":\"5\",\"orgId\":\"1010\",\"familyMemberNum\":0,\"childRenNum\":0,\"memo\":\"运作二部\",\"memberStatus\":\"0\",\"createTime\":\"Apr 16, 2015 4:10:36 PM\",\"avatarUrl\":\"https://k20.st.iblimg.com/resources/v3.0/css/i/v3-head.png\",\"random\":\"1234\",\"age\":0,\"blackFlag\":0,\"mobileBindFlag\":1,\"emailBindFlag\":0,\"memberLevel\":3,\"getNums\":0,\"backNums\":0,\"isApplyCancle\":\"0\",\"idFlag\":\"0\",\"mediaCephUrl\":\"https://k20.st.iblimg.com/resources/v3.0/css/i/v3-head.png\",\"fakePeopleFlag\":0}";

        Producer<String, String> producer = new KafkaProducer(props);
        for(int i = 0; i < 100; i++) {

//            JSONObject json = new JSONObject();
//            json.put("name", "3333");
//            json.put("type", "0000");
//            producer.send(new ProducerRecord<String, String>("my-topic", s));


            String record = String.valueOf(i);
            // send() 方法是异步的，不会阻塞当前线程
//            producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
            producer.send(new ProducerRecord<String, String>("my-topic", record), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (metadata != null) {
                        System.out.println(String.format("send message: %s to topic: %s, partition: %s, offset: %s", record, metadata.topic(), metadata.partition(), metadata.offset()));
                    }
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                }
            });
            Thread.sleep(1000);
        }
        producer.close();

    }

}
