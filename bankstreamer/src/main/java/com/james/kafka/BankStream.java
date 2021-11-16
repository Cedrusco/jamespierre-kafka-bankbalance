package com.james.kafka;

import com.james.kafka.deserializer.JsonDeserializer;
import com.james.kafka.serializer.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.time.Instant;
import java.util.Properties;

public class BankStream {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "bankbalance");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.126.128:9092");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        prop.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");

        Serializer<Balance> balanceSerializer = new JsonSerializer<>();
        Serializer<Transaction> transactionSerializer = new JsonSerializer<>();
        Deserializer<Balance> balanceDeserializer = new JsonDeserializer<>(Balance.class);
        Deserializer<Transaction> transactionDeserializer = new JsonDeserializer<>(Transaction.class);
        Serde<Balance> balanceSerde = Serdes.serdeFrom(balanceSerializer, balanceDeserializer);
        Serde<Transaction> transactionSerde = Serdes.serdeFrom(transactionSerializer, transactionDeserializer);

        Balance initialBalance = new Balance();
        initialBalance.setCount(0);
        initialBalance.setBalance(0);
        initialBalance.setTime(Instant.ofEpochMilli(0L).toString());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Transaction> input = builder.stream("bank-transaction-input", Consumed.with(Serdes.String(), transactionSerde));
        KGroupedStream<String, Transaction> grouped = input
                .filter((key, value) -> value != null)
                .groupByKey(Grouped.with(Serdes.String(), transactionSerde));

        KTable<String, Balance> balanceTable = grouped
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, oldBalance) -> {
                            Balance newBalance = new Balance();
                            newBalance.setCount(oldBalance.getCount() + 1);
                            newBalance.setBalance(oldBalance.getBalance() + transaction.getAmount());

                            long transactionEpoch = Instant.parse(transaction.getTime()).toEpochMilli();
                            long balanceEpoch = Instant.parse(oldBalance.getTime()).toEpochMilli();
                            newBalance.setTime(Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch)).toString());

                            return newBalance;
                        },
                        Materialized.<String, Balance>as(Stores.persistentKeyValueStore("bank-balance-store")).with(Serdes.String(), balanceSerde));

        balanceTable.toStream().to("bank-transaction-output", Produced.with(Serdes.String(), balanceSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), prop);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
