package com.example.consumer;

import com.example.model.MyMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service // Аннотация, указывающая, что этот класс является сервисом Spring и управляется контейнером Spring
@Slf4j // Аннотация Lombok, которая автоматически создает объект логирования (log) для этого класса
public class SingleMessageConsumer {

  // Метод, помеченный как слушатель Kafka
  @KafkaListener(
    topics = "${kafka.topic}", // Указывает тему, из которой будут собираться сообщения (в конфигурации)
    groupId = "${kafka.group.single}", // Идентификатор группы потребителей (в конфигурации)
    containerFactory = "singleMessageKafkaListenerContainerFactory" // Указывает фабрику контейнеров для обработки сообщений
  )
  public void consume(MyMessage message) {
    // Логируем сообщение, которое было получено
    log.info("SingleMessageConsumer received message: {}", message);
    
    // Обрабатываем сообщение
    log.info("SingleMessageConsumer processing message: {}", message);
    // Вы можете добавить дополнительный код обработки здесь
  }

}
