package com.example.config;

import com.example.model.MyMessage;
import com.example.serializers.MyMessageDeserializer;
import com.example.serializers.MyMessageSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka // Включает поддержку Kafka в приложении Spring
public class KafkaConfig {

	// Получаем адреса bootstrap-серверов из файла свойств
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

	// Получаем ID группы для однопоточного потребителя из файла свойств
    @Value("${kafka.group.single}")
    private String singleConsumerGroupId;

	// Получаем ID группы для пакетного потребителя из файла свойств
    @Value("${kafka.group.batch}")
    private String batchConsumerGroupId;

	// Настройки для Producer (производителя сообщений)
    @Bean
    public ProducerFactory < String, MyMessage > producerFactory() {
        Map < String, Object > configProps = new HashMap < > ();
        // Указываем адреса bootstrap-серверов
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Указываем сериализатор для ключа
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Указываем сериализатор для значения, в данном случае MyMessage
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyMessageSerializer.class);
        // Гарантия доставки: "all" означает что все реплики должны подтвердить получение
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        // Указываем количество повторных попыток на случай сбоя
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        // Возвращаем фабрику производителей с заданными свойствами
        return new DefaultKafkaProducerFactory < > (configProps);
    }

	// Создаем KafkaTemplate для отправки сообщений
    @Bean
    public KafkaTemplate < String, MyMessage > kafkaTemplate() {
        // Использует producerFactory для работы с Kafka
        return new KafkaTemplate < > (producerFactory());
    }

	// Настройки для однопоточного потребителя сообщений
    @Bean
    public ConsumerFactory < String, MyMessage > singleMessageConsumerFactory() {
        Map < String, Object > props = new HashMap < > ();
        // Указываем адреса bootstrap-серверов
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Указываем ID группы для потребителя
        props.put(ConsumerConfig.GROUP_ID_CONFIG, singleConsumerGroupId);
        // Указываем десериализатор для ключа
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Указываем десериализатор для значения, в данном случае MyMessage
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyMessageDeserializer.class);
        // Включаем автоматическое подтверждение (auto-commit) для упрощения обработки
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // Возвращаем фабрику потребителей для однопоточной обработки сообщений
        return new DefaultKafkaConsumerFactory < > (props, new StringDeserializer(), new MyMessageDeserializer());
    }

	// Настройка контейнера для прослушивания для однопоточных потребителей
    @Bean
    public ConcurrentKafkaListenerContainerFactory < String, MyMessage > singleMessageKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory < String, MyMessage > factory = new ConcurrentKafkaListenerContainerFactory < > ();
        // Устанавливаем фабрику потребителей
        factory.setConsumerFactory(singleMessageConsumerFactory());
        return factory;
    }

	// Настройки для пакетного потребителя сообщений
    @Bean
    public ConsumerFactory < String, MyMessage > batchMessageConsumerFactory() {
        Map < String, Object > props = new HashMap < > ();
        // Указываем адреса bootstrap-серверов
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Указываем ID группы для потребителя
        props.put(ConsumerConfig.GROUP_ID_CONFIG, batchConsumerGroupId);
        // Указываем десериализатор для ключа
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Указываем десериализатор для значения, в данном случае MyMessage
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyMessageDeserializer.class);
		// Отключаем автоматическое подтверждение для пакетной обработки
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Указываем, сколько сообщений мы хотим получить за один раз
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        // Указываем размер минимального пакета данных, который потребитель будет ожидать
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024); // Минимум 1KB данных за одну выборку
        // Максимальное время ожидания между получениями данных
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500); // Ждать до 500ms
        // Возвращаем фабрику потребителей для пакетной обработки сообщений
        return new DefaultKafkaConsumerFactory < > (props, new StringDeserializer(), new MyMessageDeserializer());
    }
    @Bean
    public ConcurrentKafkaListenerContainerFactory < String, MyMessage > batchMessageKafkaListenerContainerFactory() {
        // Создаем новый экземпляр ConcurrentKafkaListenerContainerFactory
        ConcurrentKafkaListenerContainerFactory < String, MyMessage > factory = new ConcurrentKafkaListenerContainerFactory < > ();
        // Устанавливаем фабрику потребителей, определенную для пакетного потребителя
        factory.setConsumerFactory(batchMessageConsumerFactory());
        // Включаем пакетное прослушивание, что означает, что обработчик сможет получать
        // несколько сообщений за раз вместо одного
        factory.setBatchListener(true);
        // Возвращаем созданную фабрику контейнеров для прослушивания пакетных сообщений
        return factory;
    }
}