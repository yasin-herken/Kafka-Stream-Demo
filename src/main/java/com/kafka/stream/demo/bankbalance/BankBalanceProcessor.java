package com.kafka.stream.demo.bankbalance;


import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
public class BankBalanceProcessor{
	private static final Serde<String> STRING_SERDE = Serdes.String();

	@Autowired
	void buildPipeline(StreamsBuilder streamsBuilder){
		KStream<String, JsonNode> messageStream = streamsBuilder.stream(
				"bank-transactions",
				Consumed.with(
						STRING_SERDE,
						new JsonSerde<>(JsonNode.class)
				)
		);

		ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
		initialBalance.put("count", 0);
		initialBalance.put("balance", 0);
		initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

		KTable<String, JsonNode> bankBalance = messageStream
				.groupByKey(Grouped.with(STRING_SERDE, new JsonSerde<>(JsonNode.class)))
				.aggregate(
						() -> initialBalance,
						(key, transaction, balance) -> {
							ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
							newBalance.put("count", balance.get("count").asInt() + 1);
							newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());

							long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
							long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
							newBalance.put("time", Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch)).toString());
							return newBalance;
						},
						Named.as("bank-balance-agg"),
						Materialized.with(STRING_SERDE, new JsonSerde<>(JsonNode.class))
				);
		bankBalance.toStream().to(
				"bank-balance-exactly-once",
				Produced.with(STRING_SERDE, new JsonSerde<>(JsonNode.class))
		);
	}
}
