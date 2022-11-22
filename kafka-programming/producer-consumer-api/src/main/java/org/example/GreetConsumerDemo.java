package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class GreetConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(GreetConsumerDemo.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "foo-group");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "greet-consumer");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class);

        //properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,1);

        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();
        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();
            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

//        GreetConsumerRebalanceListener listener = new GreetConsumerRebalanceListener();

        try {
            consumer.subscribe(Arrays.asList("greet-topic")/*,listener*/);
            while (true) {
//                log.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                log.info("received " + records.count() + " records");
                Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(2);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info("Key: " + record.key() + ", Value: " + record.value() + " Partition: " + record.partition() + ", Offset: " + record.offset());
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    OffsetAndMetadata offsetMetadata = new OffsetAndMetadata(record.offset() + 1, null);
                    currentOffsets.put(topicPartition, offsetMetadata);
                }
                consumer.commitSync(currentOffsets);
                currentOffsets.clear();
                //consumer.commitAsync();
            }
        } catch (WakeupException e) {
            log.info("Wake up exception! " + e);
        } catch (Exception e) {
            log.error("Unexpected exception " + e);
        } finally {
            consumer.close();
            log.info("The consumer is now gracefully closed");
        }


    }
}
