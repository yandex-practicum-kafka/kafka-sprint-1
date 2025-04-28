package com.example.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.model.MyMessage;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyMessageDeserializer implements Deserializer<MyMessage> {

	private final ObjectMapper objectMapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(MyMessageDeserializer.class);

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public MyMessage deserialize(String topic, byte[] data) {
		try {
			if (data == null || data.length == 0) {
				logger.warn("Deserializing null or empty message. Returning null.");
				return null;
			}
			return objectMapper.readValue(data, MyMessage.class);
		} catch (Exception e) {
			logger.error("Error deserializing message: {}", new String(data), e);
			return null; // or throw SerializationException if you want to stop the consumer
		}
	}

	@Override
	public void close() {
	}
}
