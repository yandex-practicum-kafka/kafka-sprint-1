package com.example.consumer;

import com.example.model.MyMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class SingleMessageConsumer {

	@KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group.single}", containerFactory = "singleMessageKafkaListenerContainerFactory")
	public void consume(MyMessage message) {
		log.info("SingleMessageConsumer received message: {}", message);
		// Process the message
		log.info("SingleMessageConsumer processing message: {}", message);
	}

}
