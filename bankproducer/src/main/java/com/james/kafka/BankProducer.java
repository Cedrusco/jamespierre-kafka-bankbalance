package com.james.kafka;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.Random;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BankProducer {

    // Create a Producer that outputs ~ 100 messages per second to a topic.
    //      - Message has random amount (positive value)
    //      - output evenly transactions for 6 customers
    //      - data should look like { "Name":"John", "amount": 123, "time": "2017-07-19T05:24:52" }
    public static void main( String[] args )
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.126.128:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        long taskTime;
        long sleepTime = 10L; // Sleep for 10 ms to limit

        Random random = new Random();

        try {
            // limit loop to 100 times per second.
            while (true) {
                taskTime = System.currentTimeMillis();

                producer.send(createTransactionRecord(createTransactionString("James", random.nextInt(10000) + 1, getCurrentDate())));
                producer.send(createTransactionRecord(createTransactionString("Ryan", random.nextInt(10000) + 1, getCurrentDate())));
                producer.send(createTransactionRecord(createTransactionString("Steven", random.nextInt(10000) + 1, getCurrentDate())));
                producer.send(createTransactionRecord(createTransactionString("Krystal", random.nextInt(10000) + 1, getCurrentDate())));
                producer.send(createTransactionRecord(createTransactionString("Amy", random.nextInt(10000) + 1, getCurrentDate())));
                producer.send(createTransactionRecord(createTransactionString("Zoe", random.nextInt(10000) + 1, getCurrentDate())));

                taskTime = System.currentTimeMillis() - taskTime;
                if (sleepTime - taskTime > 0) {
                    Thread.sleep(sleepTime - taskTime);
                }
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        finally {
            producer.close();
        }
    }

    private static String getCurrentDate() {
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS));
    }

    private static Transaction createTransactionString(String name, int amount, String date) {
        return new Transaction(name, amount, date);
    }

    private static ProducerRecord<String, String> createTransactionRecord(Transaction transaction) {
        Gson gson = new Gson();
        return new ProducerRecord<>("bank-transaction-input", transaction.getName(), gson.toJson(transaction));
    }
}
