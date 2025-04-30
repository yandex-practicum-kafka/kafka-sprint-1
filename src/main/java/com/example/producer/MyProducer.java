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

@Service // Аннотация, указывающая, что этот класс является сервисом Spring и будет
// управляться контейнером Spring
@RequiredArgsConstructor // Аннотация Lombok, автоматически генерирующая конструктор для всех final полей
public class MyProducer {
    private static final Logger logger = LoggerFactory.getLogger(MyProducer.class); // Создание логгера для класса

	private final KafkaTemplate < String, MyMessage > kafkaTemplate; // KafkaTemplate для отправки сообщений. Использует
    // обобщения: ключ - String, значение - MyMessage.

	@Value("${kafka.topic}") // Получение имени темы Kafka из конфигурационного файла
    private String topic; // Переменная для хранения имени темы

	// Метод для отправки сообщения в Kafka
    public void sendMessage(MyMessage message) {
        logger.info("Sending message: {}", message); // Логируем отправляемое сообщение
    
		// Отправляем сообщение и получаем CompletableFuture для обработки результата
        // отправки
        CompletableFuture < SendResult < String, MyMessage >> future = kafkaTemplate.send(topic, message);
 
		// Обрабатываем успешную отправку
        future.thenAccept(success -> logger.info("Message sent successfully to topic: {}", topic)).exceptionally(ex -> {
            // Обрабатываем исключение в случае неудачной отправки
            logger.error("Failed to send message to topic: {}", topic, ex);
            return null; // Завершаем обработку исключения
        });
    }
}