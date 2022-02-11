package com.jinfa.kafka.servcie;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerSample {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        try {
            new Thread(() -> {
                try {
                    System.out.println("======================begin======================");
                    for (int i = 0; i < 10; i++) {
                        String msg = "Message " + i;
                        kafkaProducer.send(new ProducerRecord<>("TestTopic", "hello", msg),new DemoProducerCallback());
                    }

                    System.out.println("======================over======================");
                }catch (Exception o) {
                    o.printStackTrace();
                }

            }).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Thread.sleep(5000);
        new Thread(() -> {
            System.out.println("hahahah");
        }).start();


    }

    private static class DemoProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                exception.printStackTrace();
            }
        }
    }
}
