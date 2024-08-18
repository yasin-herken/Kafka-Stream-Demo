package com.kafka.stream.demo.bankbalance;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicsConfig{

	public NewTopic bankTransactions(){
		return TopicBuilder.name("bank-transactions")
				.partitions(1)
				.replicas(1)
				.build();
	}

	public NewTopic bankBalanceExactlyOnce(){
		return TopicBuilder.name("bank-balance-exactly-once")
				.partitions(1)
				.replicas(1)
				.compact()
				.build();
	}
}
