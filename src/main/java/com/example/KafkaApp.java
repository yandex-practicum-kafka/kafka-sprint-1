package com.example;

import com.example.model.MyMessage;
import com.example.producer.MyProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import java.util.Random;

@SpringBootApplication
@EnableScheduling
@RequiredArgsConstructor
public class KafkaApp {

	private final MyProducer producer;
	private final Random random = new Random();

	public static void main(String[] args) {
		SpringApplication.run(KafkaApp.class, args);
	}

	@Scheduled(fixedRate = 1000) // Send message every 1 second
	public void produceMessage() {
		int id = random.nextInt(100);
		String messageContent = "Message with id: " + id;
		MyMessage message = new MyMessage(messageContent, id);
		producer.sendMessage(message);
	}
}
