package com.example.producer;

import com.example.model.MyMessage;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class MyProducer {

    private static final Logger logger = LoggerFactory.getLogger(MyProducer.class);

    private final KafkaTemplate<String, MyMessage> kafkaTemplate;

    @Value("${kafka.topic}")
    private String topic;

    public void sendMessage(MyMessage message) {
        logger.info("Sending message: {}", message);
        CompletableFuture<SendResult<String, MyMessage>> future = kafkaTemplate.send(topic, message);
        future.thenAccept(success -> logger.info("Message sent successfully to topic: {}", topic))
              .exceptionally(ex -> {
                  logger.error("Failed to send message to topic: {}", topic, ex);
                  return null;
              });
    }
}