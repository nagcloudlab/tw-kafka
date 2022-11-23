package com.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.sql.Time;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableKafka
public class SpringbootApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootApplication.class, args);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Component
    private class Producer {
        @EventListener(ApplicationStartedEvent.class)
        public void sendMessage() throws InterruptedException {
            for (int i = 0; i < 1000; i++) {
                kafkaTemplate.send("greet-topic", "HelloWorld");
                TimeUnit.SECONDS.sleep(1);
            }
        }
    }

    @Component
    private class Consumer {
        @KafkaListener(topics = "greet-topic",groupId = "foo-group")
        public void consumeMessage(String message) {
            System.out.println("Received: " + message);
        }
    }

}
