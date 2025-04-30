package com.example.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.model.MyMessage;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class MyMessageSerializer implements Serializer < MyMessage > {
    // Создаем экземпляр ObjectMapper для преобразования объекта MyMessage в байтовый массив
    private final ObjectMapper objectMapper = new ObjectMapper();
    // Создаем логгер для этого класса
    private static final Logger logger = LoggerFactory.getLogger(MyMessageSerializer.class);

		// Метод конфигурации, вызывается перед использованием сериализатора
		@Override
    public void configure(Map < String, ? > configs, boolean isKey) {
        // Здесь могут быть установлены дополнительные параметры, если требуется
    }

		// Метод для сериализации объекта MyMessage в байтовый массив
    @Override
    public byte[] serialize(String topic, MyMessage data) {
        try {
            // Проверяем, не является ли переданный объект null
            if (data == null) {
                logger.warn("Serializing null message. Returning null byte array.");
                return null; // Возвращаем null, если объект отсутствует
            }
            // Преобразуем объект MyMessage в байтовый массив с помощью ObjectMapper
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            // Логируем ошибку, если сериализация не удалась
            logger.error("Error serializing message: {}", data, e);
            return null; // Возвращаем null или можно выбросить SerializationException, если необходимо прекратить работу производителя
        }
    }

		// Метод для освобождения ресурсов, когда сериализатор больше не требуется
    @Override
    public void close() {
        // Здесь можно освободить ресурсы, если они были выделены
    }
}