package com.example;

import com.example.types.PosInvoice;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class RewardsPartitioner implements StreamPartitioner<String, PosInvoice> {
    @Override
    public Integer partition(String s, String s2, PosInvoice posInvoice, int numPartitions) {
        int i = Math.abs(posInvoice.getCustomerCardNo().hashCode()) % numPartitions;
        return i;
    }
}
