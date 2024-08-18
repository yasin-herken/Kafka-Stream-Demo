package com.kafka.stream.demo.bankbalance;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class BankProducerProducer{

	@Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
	private String bootstrapAddress;

	@Bean
	public ProducerFactory<String, String> producerFactory(){
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		configProps.put(
				ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				org.apache.kafka.common.serialization.StringSerializer.class
		);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
										org.apache.kafka.common.serialization.StringSerializer.class);
		// producer acks
		configProps.put(ProducerConfig.ACKS_CONFIG, "all");
		// how many times to retry when produce request fails
		configProps.put(ProducerConfig.RETRIES_CONFIG, 3);

		// how long to wait before sending messages in batch
		configProps.put(ProducerConfig.LINGER_MS_CONFIG, "1");

		// enable idempotence
		configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate(){
		return new KafkaTemplate<>(producerFactory());
	}
}
