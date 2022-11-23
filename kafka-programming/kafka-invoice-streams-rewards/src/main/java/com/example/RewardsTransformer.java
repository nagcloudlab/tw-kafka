package com.example;

import com.example.types.Notification;
import com.example.types.PosInvoice;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class RewardsTransformer implements ValueTransformer<PosInvoice, Notification> {

    private KeyValueStore<String, Double> stateStore;

    @Override
    public void init(ProcessorContext processorContext) {
        this.stateStore = processorContext.getStateStore(AppConfigs.REWARDS_STORE_NAME);
    }

    @Override
    public Notification transform(PosInvoice posInvoice) {
        Notification notification = new Notification();
        notification.setInvoiceNumber(posInvoice.getInvoiceNumber());
        notification.setCustomerCardNo(posInvoice.getCustomerCardNo());
        notification.setTotalAmount(posInvoice.getTotalAmount());
        notification.setEarnedLoyaltyPoints(posInvoice.getTotalAmount() * AppConfigs.LOYALTY_FACTOR);
        notification.setTotalLoyaltyPoints(0.0);
        Double accumulatedRewards = stateStore.get(notification.getCustomerCardNo());
        Double totalRewards;
        if (accumulatedRewards != null)
            totalRewards = accumulatedRewards + notification.getEarnedLoyaltyPoints();
        else
            totalRewards = notification.getEarnedLoyaltyPoints();
        stateStore.put(notification.getCustomerCardNo(), totalRewards);
        notification.setTotalLoyaltyPoints(totalRewards);

        return notification;
    }

    @Override
    public void close() {

    }
}
