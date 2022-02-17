package com.jinfa.kafka.servcie;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import static com.jinfa.kafka.servcie.ConsumerSample.initConfig;

//代码清单14-2 第三种多线程消费实现方式 一个消费者，负责拉消息，多个线程来处理消息
public class ThirdMultiConsumerThreadDemo {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";

    //省略initConfig()方法，具体请参考代码清单14-1
    public static void main(String[] args) {
        Properties props = initConfig();
        KafkaConsumerThread consumerThread = new KafkaConsumerThread(props, topic, Runtime.getRuntime().availableProcessors());
        consumerThread.start();
    }

    public static class KafkaConsumerThread extends Thread {
        private final KafkaConsumer<String, String> kafkaConsumer;
        private final ExecutorService executorService;
        public static final Map<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();
        public static final Map<TopicPartition, OffsetAndMetadata> maxOffsets = new ConcurrentHashMap<>();

        public KafkaConsumerThread(Properties props, String topic, int threadNumber) {
            kafkaConsumer = new KafkaConsumer<>(props);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            executorService = new ThreadPoolExecutor(threadNumber, threadNumber,
                    1000L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy());
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        executorService.submit(new RecordsHandler(records, offsets));
                    }
                    synchronized (offsets) {
                        if (!offsets.isEmpty()) {
                            for (TopicPartition tp : offsets.keySet()) {
                                if (maxOffsets.get(tp) == null) {
                                    maxOffsets.put(tp, offsets.get(tp));
                                }else {
                                    // 如果当前提交的位移小于历史提交位移，则不再提交
                                    if (offsets.get(tp).offset() < maxOffsets.get(tp).offset()) {
                                        offsets.remove(tp);
                                    }else {
                                        maxOffsets.put(tp, offsets.get(tp));
                                    }
                                }
                            }
                            kafkaConsumer.commitSync(offsets);
                            offsets.clear();
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }

    }

    public static class RecordsHandler extends Thread{
        public final ConsumerRecords<String, String> records;
        public Map<TopicPartition, OffsetAndMetadata> offsets = null;

        public RecordsHandler(ConsumerRecords<String, String> records, Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.records = records;
            this.offsets = offsets;
        }

        @Override
        public void run(){
            for (TopicPartition tp : records.partitions()) {
                List<ConsumerRecord<String, String>> tpRecords = records.records(tp);
                //处理tpRecords.
                long lastConsumedOffset = tpRecords.get(tpRecords.size() - 1).offset();
                synchronized (offsets) {
                    if (!offsets.containsKey(tp)) {
                        offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
                    }else {
                        long position = offsets.get(tp).offset();
                        if (position < lastConsumedOffset + 1) {
                            offsets.put(tp, new OffsetAndMetadata(lastConsumedOffset + 1));
                        }
                    }
                }
            }
        }
    }
}
