package com.example.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.model.MyMessage;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyMessageDeserializer implements Deserializer < MyMessage > {

	// Создаем экземпляр ObjectMapper для преобразования байтовых данных в объект MyMessage
    private final ObjectMapper objectMapper = new ObjectMapper();
    // Создаем логгер для этой класса
    private static final Logger logger = LoggerFactory.getLogger(MyMessageDeserializer.class);
    
		// Метод конфигурации, вызывается перед использованием десериализатора
		@Override
    public void configure(Map < String, ? > configs, boolean isKey) {
        // Здесь могут быть установлены дополнительные параметры, если требуется
    }

		// Метод для десериализации сообщения из байтового массива в объект MyMessage
    @Override
    public MyMessage deserialize(String topic, byte[] data) {
        try {
            // Проверяем, не является ли полученный массив данных пустым или равным null
            if (data == null || data.length == 0) {
                logger.warn("Deserializing null or empty message. Returning null.");
                return null; // Возвращаем null, если данные отсутствуют
            }
            // Преобразуем байтовый массив в объект MyMessage с помощью ObjectMapper
            return objectMapper.readValue(data, MyMessage.class);
        } catch (Exception e) {
            // Логируем ошибку, если десериализация не удалась
            logger.error("Error deserializing message: {}", new String(data), e);
            return null; // Возвращаем null или можно выбросить SerializationException, если необходимо прекратить работу потребителя
        }
    }

		// Метод для освобождения ресурсов, когда десериализатор больше не требуется
    @Override
    public void close() {
        // Здесь можно освободить ресурсы, если они были выделены
    }
}