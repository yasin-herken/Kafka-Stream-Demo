package com.kafka.stream.demo.bankbalance;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class BankBalanceApplication{

	public static void main(String[] args){
		ApplicationContext context = SpringApplication.run(BankBalanceApplication.class, args);
		KafkaTemplate<String, String> kafkaTemplate = context.getBean(KafkaTemplate.class);
		int i = 0;
		while(true){
			System.out.println("Producing batch: " + i);
			try{
				kafkaTemplate.send(newRandomTransaction("john"));
				Thread.sleep(1000);
				kafkaTemplate.send(newRandomTransaction("stephane"));
				Thread.sleep(1000);
				kafkaTemplate.send(newRandomTransaction("alice"));
				Thread.sleep(1000);

				i++;
			}
			catch(InterruptedException e){
				break;
			}
		}
		kafkaTemplate.destroy();
	}

	private static ProducerRecord<String, String> newRandomTransaction(String name){
		// creates an empty event
		ObjectNode transaction = JsonNodeFactory.instance.objectNode();

		Integer amount = ThreadLocalRandom.current().nextInt(0, 100);
		Instant now = Instant.now();
		transaction.put("name", name);
		transaction.put("amount", amount);
		transaction.put("time", now.toString());
		return new ProducerRecord<>("bank-transactions", name, transaction.toString());
	}

}
