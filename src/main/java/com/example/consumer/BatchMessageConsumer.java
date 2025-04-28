package com.example.consumer;

import com.example.model.MyMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class BatchMessageConsumer {

	@KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group.batch}", containerFactory = "batchMessageKafkaListenerContainerFactory")
	public void consume(List<MyMessage> messages) {
		log.info("BatchMessageConsumer received batch of {} messages", messages.size());
		for (MyMessage message : messages) {
			// Process each message in the batch
			log.info("BatchMessageConsumer processing message: {}", message);
		}
	}

}
