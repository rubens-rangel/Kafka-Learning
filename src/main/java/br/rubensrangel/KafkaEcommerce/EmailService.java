package br.rubensrangel.KafkaEcommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class EmailService {

    public static void main(String[] args) throws IOException {
        var emailService = new EmailService();
        try (var service = new KafkaService(EmailService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL", emailService::parse)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("--------");
        System.out.println("Enviando Email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Email Enviado");
    }

}
