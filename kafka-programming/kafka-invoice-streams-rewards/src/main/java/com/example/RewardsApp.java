package com.example;

import com.example.serde.AppSerdes;
import com.example.types.PosInvoice;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class RewardsApp {

    private static final Logger logger = LoggerFactory.getLogger("streams-with-store");

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, PosInvoice> KS0 =
                builder.stream(AppConfigs.posTopicName, Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice()))
                        .filter((key, value) -> value.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME));

        StoreBuilder kvStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(AppConfigs.REWARDS_STORE_NAME),
                AppSerdes.String(), AppSerdes.Double()
        ).withCachingEnabled();

        builder.addStateStore(kvStoreBuilder);

        KStream KS1=KS0.through(AppConfigs.REWARDS_TEMP_TOPIC,
                Produced.with(AppSerdes.String(),AppSerdes.PosInvoice(),new RewardsPartitioner()));

        KS0.transformValues(() -> new RewardsTransformer(), AppConfigs.REWARDS_STORE_NAME)
                .to(AppConfigs.notificationTopic, Produced.with(AppSerdes.String(), AppSerdes.Notification()));

        logger.info("Starting Stream");
        KafkaStreams stream = new KafkaStreams(builder.build(), props);
        stream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Streams");
            stream.close();
        }));

    }
}
