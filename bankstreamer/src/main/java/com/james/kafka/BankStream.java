package com.james.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

public class BankStream {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "bankbalance");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.126.128:9092");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        prop.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> input = builder.stream("bank-transaction-input", Consumed.with(Serdes.String(), Serdes.String()));
        KGroupedStream<String, String> grouped = input
                .filter((key, value) -> value != null)
                .groupByKey();

        KTable<String, String> balance = grouped
                .aggregate(
                        () -> "{}",
                        (key, newValue, aggValue) -> {
                            Gson gson = new Gson();
                            Transaction transaction = gson.fromJson(newValue, Transaction.class);
                            Transaction aggTransaction = gson.fromJson(aggValue, Transaction.class);
                            aggTransaction.setAmount(aggTransaction.getAmount() + transaction.getAmount());
                            if (aggTransaction.getName() == null) aggTransaction.setName(transaction.getName());
                            if (aggTransaction.getTime() == null) {
                                aggTransaction.setTime(transaction.getTime());
                            }
                            else {
                                try {
                                    Date start = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH).parse(aggTransaction.getTime());
                                    Date end = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH).parse(transaction.getTime());

                                    if (start.compareTo(end) < 0) {
                                        aggTransaction.setTime(transaction.getTime());
                                    }
                                }
                                catch (Exception ex) {
                                    ex.printStackTrace();
                                }
                            }
                            return gson.toJson(aggTransaction);
                        },
                        Materialized.<String, String>as(Stores.persistentKeyValueStore("bank-balance-store")).with(Serdes.String(), Serdes.String()));

        balance.toStream().to("bank-transaction-output", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), prop);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
