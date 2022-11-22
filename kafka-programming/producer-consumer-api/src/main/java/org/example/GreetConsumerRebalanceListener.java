package org.example;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class GreetConsumerRebalanceListener implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        System.out.println("GreetConsumerRebalanceListener::onPartitionsRevoked");

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        System.out.println("GreetConsumerRebalanceListener::onPartitionsAssigned");
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        ConsumerRebalanceListener.super.onPartitionsLost(partitions);
    }
}
