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
@EnableKafka
public class KafkaConfig {

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${kafka.group.single}")
	private String singleConsumerGroupId;

	@Value("${kafka.group.batch}")
	private String batchConsumerGroupId;

	// Producer Configuration
	@Bean
	public ProducerFactory<String, MyMessage> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyMessageSerializer.class);
		configProps.put(ProducerConfig.ACKS_CONFIG, "all"); // At least once delivery guarantee
		configProps.put(ProducerConfig.RETRIES_CONFIG, 3); // Retry up to 3 times

		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<String, MyMessage> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	// Single Message Consumer Configuration
	@Bean
	public ConsumerFactory<String, MyMessage> singleMessageConsumerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, singleConsumerGroupId);

		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyMessageDeserializer.class);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // Enable auto-commit
		return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new MyMessageDeserializer());
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, MyMessage> singleMessageKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, MyMessage> factory =
			new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(singleMessageConsumerFactory());
		return factory;
	}

	// Batch Message Consumer Configuration
	@Bean
	public ConsumerFactory<String, MyMessage> batchMessageConsumerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, batchConsumerGroupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyMessageDeserializer.class);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Disable auto-commit for batch processing
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10); // Minimum 10 messages per poll
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024); // Minimum 1KB data per fetch
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500); // Wait up to 500ms for data

		return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new MyMessageDeserializer());
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, MyMessage> batchMessageKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, MyMessage> factory =
			new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(batchMessageConsumerFactory());
		factory.setBatchListener(true); // Enable batch listening
		return factory;
	}
}
