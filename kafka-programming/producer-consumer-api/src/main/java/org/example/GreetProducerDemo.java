package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class GreetProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(GreetProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a Kafka Producer");

        Properties properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "greet-producer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // SAFE PRODUCER
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        properties.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 100);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // HIGH THROUGHPUT PRODUCER
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);


        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);


        //properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, GreetTopicPartitioner.class.getName());
        //properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, GreetProducerInterceptor.class.getName());


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>(
                            "greet-topic",
                            null,
                            "key-" + i,
                            "Hello World!"
                    );
            producer.send(producerRecord, (metadata, err) -> {
                if (err == null) {
                    log.info("Received new metadata. \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Key: " + producerRecord.key() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());
                } else {
                    log.error("Error while Producing ", err);
                }
            });

            try {
                TimeUnit.MILLISECONDS.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        producer.flush();
        producer.close();


    }

}
