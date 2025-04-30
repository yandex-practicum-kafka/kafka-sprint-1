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

	@Scheduled(fixedRate = 1000) // Аннотация, указывающая, что метод будет вызываться каждые 1000 миллисекунд (1 секунда)
	public void produceMessage() {
		// Генерируем случайный идентификатор в диапазоне от 0 до 99
		int id = random.nextInt(100);
		
		// Формируем текст сообщения, включая сгенерированный идентификатор
		String messageContent = "Message with id: " + id;
		
		// Создаем новый объект MyMessage, который содержит текст сообщения и идентификатор
		MyMessage message = new MyMessage(messageContent, id);
		
		// Отправляем созданное сообщение с помощью метода sendMessage из продюсера
		producer.sendMessage(message);
	}
	}
