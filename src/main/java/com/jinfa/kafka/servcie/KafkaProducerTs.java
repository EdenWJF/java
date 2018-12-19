package com.jinfa.kafka.servcie;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerTs {

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        try {
            new Thread(() -> {
                try {
                    System.out.println("======================begin");
                    for (int i = 0; i < 100; i++) {
                        String msg = "Message " + i;
                        kafkaProducer.send(new ProducerRecord<>("HelloWorld", msg),new DemoProducerCallback());
                    }

                    System.out.println("======================over");
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
