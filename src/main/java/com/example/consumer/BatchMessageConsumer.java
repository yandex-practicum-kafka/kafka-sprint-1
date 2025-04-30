package com.example.consumer;
import com.example.model.MyMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.util.List;

@Service // Аннотация, указывающая, что этот класс является сервисом Spring и управляется контейнером Spring
@Slf4j // Аннотация Lombok для генерации логирования (log) в этом классе
public class BatchMessageConsumer {
    // Метод, помеченный как слушатель Kafka
    @KafkaListener(topics = "${kafka.topic}", // Имя темы, из которой будет производиться получение сообщений, задается в конфигурации
        groupId = "${kafka.group.batch}", // Идентификатор группы потребителей, задается в конфигурации
        containerFactory = "batchMessageKafkaListenerContainerFactory" // Указывает, какой контейнер использовать для пакетной обработки
    )
    public void consume(List < MyMessage > messages) {
        // Логируем информацию о размере полученного батча
        log.info("BatchMessageConsumer received batch of {} messages", messages.size());
        // Проходим по каждому сообщению в полученном пакете
        for (MyMessage message: messages) {
            // Обрабатываем каждое сообщение в пакете
            log.info("BatchMessageConsumer processing message: {}", message);
        }
    }
}