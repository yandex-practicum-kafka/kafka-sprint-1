package com.example.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.model.MyMessage;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyMessageSerializer implements Serializer<MyMessage> {

	private final ObjectMapper objectMapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(MyMessageSerializer.class);

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public byte[] serialize(String topic, MyMessage data) {
		try {
			if (data == null) {
				logger.warn("Serializing null message. Returning null byte array.");
				return null;
			}
			return objectMapper.writeValueAsBytes(data);
		} catch (Exception e) {
			logger.error("Error serializing message: {}", data, e);
			return null; // or throw SerializationException if you want to stop the producer
		}
	}

	@Override
	public void close() {
	}
}
