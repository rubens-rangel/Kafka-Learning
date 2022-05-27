package br.rubensrangel.KafkaEcommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class FraudDetectorService {

    public static void main(String[] args) throws IOException {
        var fraudService = new FraudDetectorService();
        try (var service = new KafkaService(FraudDetectorService.class.getName(), "ECOMMERCE_NEW_ORDER", fraudService::parse)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("--------");
        System.out.println("Processando nova Ordem, checando por fraudes");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Ordem Processada");
    }
}

