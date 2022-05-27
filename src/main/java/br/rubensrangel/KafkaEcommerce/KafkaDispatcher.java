package br.rubensrangel.KafkaEcommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher implements Closeable {

    private final KafkaProducer<String, String> producer;

     KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

     static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

     void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value);
        producer.send(record, getCallback()).get();
        var email = " Bem vindo! Processando sua compra!";

//        producer.send(emailRecord, getCallback()).get();
    }

     static Callback getCallback() {
        return (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            }
            System.out.println("Sucesso enviando para o topico: " + data.topic() + " ::: partition: " + data.partition() + " /offset: " + data.offset() + " /date: " + new Date(data.timestamp()));
        };
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
